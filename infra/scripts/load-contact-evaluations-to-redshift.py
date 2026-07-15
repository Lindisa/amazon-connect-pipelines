import sys
from datetime import datetime, timedelta

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql import Window
from pyspark.sql.functions import (
    col,
    lit,
    when,
    current_timestamp,
    trim,
    row_number,
    sha2,
    struct,
    to_json,
    get_json_object,
)
from pyspark.sql.types import (
    StructType,
    ArrayType,
    MapType,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    TimestampType,
    NullType,
)


# ============================================================
# Exact target/staging column order
# ============================================================
ALL_TARGET_COLUMNS = [
    "schema_version",
    "evaluation_id",
    "contact_id",
    "account_id",
    "instance_id",
    "agent_id",
    "evaluation_definition_title",
    "evaluator",
    "evaluation_definition_id",
    "evaluation_definition_version",
    "evaluation_start_timestamp",
    "evaluation_submit_timestamp",
    "evaluation_score_percentage",
    "creator",
    "auto_evaluated",
    "resubmitted",
    "evaluation_source",
    "evaluation_type",
    "calibration_session_id",
    "evaluated_participant_id",
    "evaluated_participant_role",
    "evaluation_acknowledger_comment",
    "evaluation_acknowledged_timestamp",
    "evaluation_acknowledged_by_user_name",
    "evaluation_acknowledged_by_user_id",
    "metadata",
    "sections",
    "questions",
    "source_file",
    "processed_at",
    "year",
    "month",
    "day",
]


SUPER_TARGET_COLUMNS = [
    "metadata",
    "sections",
    "questions",
]


STRING_TARGET_COLUMNS = [
    "schema_version",
    "evaluation_id",
    "contact_id",
    "account_id",
    "instance_id",
    "agent_id",
    "evaluation_definition_title",
    "evaluator",
    "evaluation_definition_id",
    "creator",
    "evaluation_source",
    "evaluation_type",
    "calibration_session_id",
    "evaluated_participant_id",
    "evaluated_participant_role",
    "evaluation_acknowledger_comment",
    "evaluation_acknowledged_by_user_name",
    "evaluation_acknowledged_by_user_id",
    "source_file",
    "year",
    "month",
    "day",
]


# ============================================================
# Optional Glue argument helper
# ============================================================
def get_optional_arg(arg_name, default_value):
    for index, argument in enumerate(sys.argv):
        if (
            argument == f"--{arg_name}"
            and index + 1 < len(sys.argv)
        ):
            return sys.argv[index + 1]

    return default_value


# ============================================================
# Build Redshift temporary S3 directory
# ============================================================
def build_redshift_tmp_dir(bucket_value):
    value = (
        bucket_value
        .strip()
        .replace("s3://", "")
        .strip("/")
    )

    if "/" in value:
        return f"s3://{value}/"

    return f"s3://{value}/redshift_temp/"


# ============================================================
# Build Glue partition predicate
# ============================================================
def build_partition_predicate(lookback_days):
    if lookback_days < 0:
        raise ValueError(
            "lookback_days cannot be negative."
        )

    today = datetime.now().date()
    conditions = []

    for days_ago in range(lookback_days + 1):
        partition_date = today - timedelta(
            days=days_ago
        )

        conditions.append(
            "("
            f"year = '{partition_date.year}' "
            f"AND month = '{partition_date.month:02d}' "
            f"AND day = '{partition_date.day:02d}'"
            ")"
        )

    return "(" + " OR ".join(conditions) + ")"


# ============================================================
# Lowercase all top-level source columns
# ============================================================
def lowercase_source_columns(source_df):
    spark_df = source_df

    original_columns = list(spark_df.columns)

    lowercase_columns = [
        column_name.lower()
        for column_name in original_columns
    ]

    if len(lowercase_columns) != len(
        set(lowercase_columns)
    ):
        raise ValueError(
            "Lowercasing source columns would create "
            "duplicate column names."
        )

    for column_name in original_columns:
        lowercase_name = column_name.lower()

        if column_name != lowercase_name:
            spark_df = spark_df.withColumnRenamed(
                column_name,
                lowercase_name,
            )

    return spark_df


# ============================================================
# Read a field from metadata whether it is a Struct or JSON
# string
# ============================================================
def metadata_value(
    spark_df,
    metadata_type,
    struct_field_name,
    json_field_name,
    target_type,
):
    if isinstance(metadata_type, StructType):
        available_fields = {
            field.name
            for field in metadata_type.fields
        }

        if struct_field_name in available_fields:
            return col(
                f"metadata.{struct_field_name}"
            ).cast(target_type)

        return lit(None).cast(target_type)

    if isinstance(metadata_type, StringType):
        return get_json_object(
            col("metadata"),
            f"$.{json_field_name}",
        ).cast(target_type)

    return lit(None).cast(target_type)


# ============================================================
# Transform Contact Evaluation source
# ============================================================
def transform_source_dataframe(source_df):
    spark_df = lowercase_source_columns(
        source_df
    )

    # --------------------------------------------------------
    # Top-level exported field names become lowercase after
    # normalisation.
    # --------------------------------------------------------
    if "schemaversion" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(
            "schemaversion",
            "schema_version",
        )
    else:
        spark_df = spark_df.withColumn(
            "schema_version",
            lit(None).cast(StringType()),
        )

    if "evaluationid" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(
            "evaluationid",
            "evaluation_id",
        )
    else:
        spark_df = spark_df.withColumn(
            "evaluation_id",
            lit(None).cast(StringType()),
        )

    # --------------------------------------------------------
    # Extract metadata fields
    # --------------------------------------------------------
    if "metadata" in spark_df.columns:
        metadata_type = (
            spark_df.schema["metadata"].dataType
        )

        metadata_fields = [
            (
                "contact_id",
                "contactId",
                "contactId",
                StringType(),
            ),
            (
                "account_id",
                "accountId",
                "accountId",
                StringType(),
            ),
            (
                "instance_id",
                "instanceId",
                "instanceId",
                StringType(),
            ),
            (
                "agent_id",
                "agentId",
                "agentId",
                StringType(),
            ),
            (
                "evaluation_definition_title",
                "evaluationDefinitionTitle",
                "evaluationDefinitionTitle",
                StringType(),
            ),
            (
                "evaluator",
                "evaluator",
                "evaluator",
                StringType(),
            ),
            (
                "evaluation_definition_id",
                "evaluationDefinitionId",
                "evaluationDefinitionId",
                StringType(),
            ),
            (
                "evaluation_definition_version",
                "evaluationDefinitionVersion",
                "evaluationDefinitionVersion",
                IntegerType(),
            ),
            (
                "evaluation_start_timestamp",
                "evaluationStartTimestamp",
                "evaluationStartTimestamp",
                TimestampType(),
            ),
            (
                "evaluation_submit_timestamp",
                "evaluationSubmitTimestamp",
                "evaluationSubmitTimestamp",
                TimestampType(),
            ),
            (
                "creator",
                "creator",
                "creator",
                StringType(),
            ),
            (
                "auto_evaluated",
                "autoEvaluated",
                "autoEvaluated",
                BooleanType(),
            ),
            (
                "resubmitted",
                "resubmitted",
                "resubmitted",
                BooleanType(),
            ),
            (
                "evaluation_source",
                "evaluationSource",
                "evaluationSource",
                StringType(),
            ),
            (
                "evaluation_type",
                "evaluationType",
                "evaluationType",
                StringType(),
            ),
            (
                "calibration_session_id",
                "calibrationSessionId",
                "calibrationSessionId",
                StringType(),
            ),
            (
                "evaluated_participant_id",
                "evaluatedParticipantId",
                "evaluatedParticipantId",
                StringType(),
            ),
            (
                "evaluated_participant_role",
                "evaluatedParticipantRole",
                "evaluatedParticipantRole",
                StringType(),
            ),
            (
                "evaluation_acknowledger_comment",
                "evaluationAcknowledgerComment",
                "evaluationAcknowledgerComment",
                StringType(),
            ),
            (
                "evaluation_acknowledged_timestamp",
                "evaluationAcknowledgedTimestamp",
                "evaluationAcknowledgedTimestamp",
                TimestampType(),
            ),
            (
                "evaluation_acknowledged_by_user_name",
                "evaluationAcknowledgedByUserName",
                "evaluationAcknowledgedByUserName",
                StringType(),
            ),
            (
                "evaluation_acknowledged_by_user_id",
                "evaluationAcknowledgedByUserId",
                "evaluationAcknowledgedByUserId",
                StringType(),
            ),
        ]

        for (
            target_name,
            struct_field_name,
            json_field_name,
            target_type,
        ) in metadata_fields:
            spark_df = spark_df.withColumn(
                target_name,
                metadata_value(
                    spark_df,
                    metadata_type,
                    struct_field_name,
                    json_field_name,
                    target_type,
                ),
            )

        # ----------------------------------------------------
        # Extract metadata.score.percentage
        # ----------------------------------------------------
        if isinstance(metadata_type, StructType):
            metadata_field_names = {
                field.name
                for field in metadata_type.fields
            }

            if "score" in metadata_field_names:
                score_type = next(
                    field.dataType
                    for field in metadata_type.fields
                    if field.name == "score"
                )

                if isinstance(score_type, StructType):
                    score_field_names = {
                        field.name
                        for field in score_type.fields
                    }

                    if "percentage" in score_field_names:
                        score_expression = col(
                            "metadata.score.percentage"
                        ).cast(DoubleType())
                    else:
                        score_expression = lit(
                            None
                        ).cast(DoubleType())
                else:
                    score_expression = lit(
                        None
                    ).cast(DoubleType())
            else:
                score_expression = lit(
                    None
                ).cast(DoubleType())

        elif isinstance(metadata_type, StringType):
            score_expression = get_json_object(
                col("metadata"),
                "$.score.percentage",
            ).cast(DoubleType())

        else:
            score_expression = lit(
                None
            ).cast(DoubleType())

        spark_df = spark_df.withColumn(
            "evaluation_score_percentage",
            score_expression,
        )

    else:
        print(
            "WARNING: metadata column is missing."
        )

        missing_metadata_columns = [
            "contact_id",
            "account_id",
            "instance_id",
            "agent_id",
            "evaluation_definition_title",
            "evaluator",
            "evaluation_definition_id",
            "creator",
            "evaluation_source",
            "evaluation_type",
            "calibration_session_id",
            "evaluated_participant_id",
            "evaluated_participant_role",
            "evaluation_acknowledger_comment",
            "evaluation_acknowledged_by_user_name",
            "evaluation_acknowledged_by_user_id",
        ]

        for column_name in missing_metadata_columns:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType()),
            )

        spark_df = spark_df.withColumn(
            "evaluation_definition_version",
            lit(None).cast(IntegerType()),
        )

        spark_df = spark_df.withColumn(
            "evaluation_start_timestamp",
            lit(None).cast(TimestampType()),
        )

        spark_df = spark_df.withColumn(
            "evaluation_submit_timestamp",
            lit(None).cast(TimestampType()),
        )

        spark_df = spark_df.withColumn(
            "evaluation_acknowledged_timestamp",
            lit(None).cast(TimestampType()),
        )

        spark_df = spark_df.withColumn(
            "evaluation_score_percentage",
            lit(None).cast(DoubleType()),
        )

        spark_df = spark_df.withColumn(
            "auto_evaluated",
            lit(None).cast(BooleanType()),
        )

        spark_df = spark_df.withColumn(
            "resubmitted",
            lit(None).cast(BooleanType()),
        )

    # --------------------------------------------------------
    # Preserve the preprocess source_file column.
    # If it is missing, add it as NULL.
    # --------------------------------------------------------
    if "source_file" not in spark_df.columns:
        spark_df = spark_df.withColumn(
            "source_file",
            lit(None).cast(StringType()),
        )

    spark_df = spark_df.withColumn(
        "processed_at",
        current_timestamp(),
    )

    # --------------------------------------------------------
    # Convert SUPER fields to valid JSON strings before
    # Redshift COPY.
    # --------------------------------------------------------
    schema_types = {
        field.name: field.dataType
        for field in spark_df.schema.fields
    }

    for column_name in SUPER_TARGET_COLUMNS:
        if column_name not in spark_df.columns:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType()),
            )

            continue

        column_type = schema_types[column_name]

        if isinstance(column_type, NullType):
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType()),
            )

        elif isinstance(
            column_type,
            (StructType, ArrayType, MapType),
        ):
            spark_df = spark_df.withColumn(
                column_name,
                when(
                    col(column_name).isNull(),
                    lit(None).cast(StringType()),
                ).otherwise(
                    to_json(col(column_name))
                ),
            )

        elif isinstance(column_type, StringType):
            spark_df = spark_df.withColumn(
                column_name,
                when(
                    col(column_name).isNull()
                    | (trim(col(column_name)) == ""),
                    lit(None).cast(StringType()),
                ).otherwise(
                    col(column_name)
                ),
            )

        else:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType()),
            )

    return spark_df


# ============================================================
# Align exact target schema
# ============================================================
def align_target_schema(spark_df):
    existing_columns = set(
        spark_df.columns
    )

    for column_name in ALL_TARGET_COLUMNS:
        if column_name in existing_columns:
            continue

        if column_name in STRING_TARGET_COLUMNS:
            target_type = StringType()

        elif column_name in SUPER_TARGET_COLUMNS:
            target_type = StringType()

        elif column_name == "evaluation_definition_version":
            target_type = IntegerType()

        elif column_name == "evaluation_score_percentage":
            target_type = DoubleType()

        elif column_name in [
            "auto_evaluated",
            "resubmitted",
        ]:
            target_type = BooleanType()

        elif column_name in [
            "evaluation_start_timestamp",
            "evaluation_submit_timestamp",
            "evaluation_acknowledged_timestamp",
            "processed_at",
        ]:
            target_type = TimestampType()

        else:
            target_type = StringType()

        spark_df = spark_df.withColumn(
            column_name,
            lit(None).cast(target_type),
        )

    for column_name in STRING_TARGET_COLUMNS:
        spark_df = spark_df.withColumn(
            column_name,
            col(column_name).cast(StringType()),
        )

    for column_name in SUPER_TARGET_COLUMNS:
        spark_df = spark_df.withColumn(
            column_name,
            col(column_name).cast(StringType()),
        )

    spark_df = spark_df.withColumn(
        "evaluation_definition_version",
        col(
            "evaluation_definition_version"
        ).cast(IntegerType()),
    )

    spark_df = spark_df.withColumn(
        "evaluation_score_percentage",
        col(
            "evaluation_score_percentage"
        ).cast(DoubleType()),
    )

    spark_df = spark_df.withColumn(
        "auto_evaluated",
        col("auto_evaluated").cast(BooleanType()),
    )

    spark_df = spark_df.withColumn(
        "resubmitted",
        col("resubmitted").cast(BooleanType()),
    )

    timestamp_columns = [
        "evaluation_start_timestamp",
        "evaluation_submit_timestamp",
        "evaluation_acknowledged_timestamp",
        "processed_at",
    ]

    for column_name in timestamp_columns:
        spark_df = spark_df.withColumn(
            column_name,
            col(column_name).cast(TimestampType()),
        )

    spark_df = spark_df.withColumn(
        "evaluation_id",
        when(
            col("evaluation_id").isNull()
            | (trim(col("evaluation_id")) == ""),
            lit(None).cast(StringType()),
        ).otherwise(
            trim(col("evaluation_id"))
        ),
    )

    return spark_df


# ============================================================
# Keep the newest row per evaluation_id
# ============================================================
def remove_duplicate_evaluations(valid_df):
    valid_df = valid_df.withColumn(
        "__row_hash",
        sha2(
            to_json(
                struct(
                    *[
                        col(column_name)
                        for column_name in ALL_TARGET_COLUMNS
                    ]
                )
            ),
            256,
        ),
    )

    duplicate_window = (
        Window
        .partitionBy("evaluation_id")
        .orderBy(
            col(
                "evaluation_submit_timestamp"
            ).desc_nulls_last(),
            col(
                "evaluation_start_timestamp"
            ).desc_nulls_last(),
            col("year").desc_nulls_last(),
            col("month").desc_nulls_last(),
            col("day").desc_nulls_last(),
            col("source_file").desc_nulls_last(),
            col("__row_hash").desc_nulls_last(),
        )
    )

    return (
        valid_df
        .withColumn(
            "__duplicate_row_number",
            row_number().over(
                duplicate_window
            ),
        )
        .filter(
            col("__duplicate_row_number") == 1
        )
        .drop(
            "__duplicate_row_number",
            "__row_hash",
        )
    )


# ============================================================
# Main Glue job
# ============================================================
def main():
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "source_table",
            "source_database",
            "target_table",
            "target_connection_name",
            "job_store_bucket_name",
            "redshift_s3_role_arn",
        ],
    )

    spark_context = SparkContext()

    glue_context = GlueContext(
        spark_context
    )

    job = Job(glue_context)

    job.init(
        args["JOB_NAME"],
        args,
    )

    target_table = args[
        "target_table"
    ]

    staging_table = (
        "public.contact_evaluations_staging"
    )

    redshift_tmp_dir = build_redshift_tmp_dir(
        args["job_store_bucket_name"]
    )

    try:
        lookback_days = int(
            get_optional_arg(
                "lookback_days",
                "1",
            )
        )
    except ValueError as error:
        raise ValueError(
            "lookback_days must be a valid integer."
        ) from error

    initial_load = (
        get_optional_arg(
            "initial_load",
            "false",
        )
        .strip()
        .lower()
        == "true"
    )

    print(
        "===== CONTACT EVALUATIONS REDSHIFT ETL STARTED ====="
    )

    print(
        f"Target table: {target_table}"
    )

    print(
        f"Staging table: {staging_table}"
    )

    print(
        f"Redshift temporary directory: {redshift_tmp_dir}"
    )

    # --------------------------------------------------------
    # Read Glue Catalog source
    # --------------------------------------------------------
    if initial_load:
        print(
            "Performing full Contact Evaluations load."
        )

        source_dyf = (
            glue_context
            .create_dynamic_frame
            .from_catalog(
                database=args[
                    "source_database"
                ],
                table_name=args[
                    "source_table"
                ],
                transformation_ctx="source_dyf",
            )
        )

    else:
        predicate = build_partition_predicate(
            lookback_days
        )

        print(
            "Performing incremental Contact Evaluations load."
        )

        print(
            f"Predicate: {predicate}"
        )

        source_dyf = (
            glue_context
            .create_dynamic_frame
            .from_catalog(
                database=args[
                    "source_database"
                ],
                table_name=args[
                    "source_table"
                ],
                push_down_predicate=predicate,
                transformation_ctx="source_dyf",
            )
        )

    source_df = source_dyf.toDF()

    source_record_count = (
        source_df.count()
    )

    print(
        "Source Contact Evaluation records read: "
        f"{source_record_count}"
    )

    if source_record_count == 0:
        print(
            "No Contact Evaluation records found."
        )

        job.commit()

        print(
            "===== CONTACT EVALUATIONS REDSHIFT ETL COMPLETED ====="
        )

        return

    transformed_df = (
        transform_source_dataframe(
            source_df
        )
    )

    prepared_df = (
        align_target_schema(
            transformed_df
        )
        .cache()
    )

    missing_evaluation_id_count = (
        prepared_df
        .filter(
            col("evaluation_id").isNull()
        )
        .count()
    )

    print(
        "Records removed because evaluation_id was "
        f"missing or blank: {missing_evaluation_id_count}"
    )

    valid_df = prepared_df.filter(
        col("evaluation_id").isNotNull()
    )

    valid_df = remove_duplicate_evaluations(
        valid_df
    )

    valid_df = (
        valid_df
        .select(
            *ALL_TARGET_COLUMNS
        )
        .cache()
    )

    valid_record_count = (
        valid_df.count()
    )

    print(
        "Valid deduplicated evaluations to load: "
        f"{valid_record_count}"
    )

    if valid_record_count == 0:
        valid_df.unpersist()
        prepared_df.unpersist()

        job.commit()

        print(
            "No valid evaluation records remain."
        )

        print(
            "===== CONTACT EVALUATIONS REDSHIFT ETL COMPLETED ====="
        )

        return

    valid_df.select(
        "evaluation_id",
        "contact_id",
        "evaluation_definition_title",
        "evaluation_submit_timestamp",
        "evaluation_score_percentage",
        "source_file",
        "year",
        "month",
        "day",
    ).show(
        10,
        truncate=False,
    )

    final_dyf = DynamicFrame.fromDF(
        valid_df,
        glue_context,
        "final_contact_evaluations_frame",
    )

    preactions = f"""
    TRUNCATE TABLE {staging_table};
    """

    # --------------------------------------------------------
    # Upsert logic:
    #
    # Delete an older copy of an evaluation, then insert the
    # newest version from staging.
    # --------------------------------------------------------
    postactions = f"""
    BEGIN;

    DELETE FROM {target_table}
    USING {staging_table}
    WHERE {target_table}.evaluation_id =
          {staging_table}.evaluation_id;

    INSERT INTO {target_table} (
        schema_version,
        evaluation_id,
        contact_id,
        account_id,
        instance_id,
        agent_id,
        evaluation_definition_title,
        evaluator,
        evaluation_definition_id,
        evaluation_definition_version,
        evaluation_start_timestamp,
        evaluation_submit_timestamp,
        evaluation_score_percentage,
        creator,
        auto_evaluated,
        resubmitted,
        evaluation_source,
        evaluation_type,
        calibration_session_id,
        evaluated_participant_id,
        evaluated_participant_role,
        evaluation_acknowledger_comment,
        evaluation_acknowledged_timestamp,
        evaluation_acknowledged_by_user_name,
        evaluation_acknowledged_by_user_id,
        metadata,
        sections,
        questions,
        source_file,
        processed_at,
        year,
        month,
        day
    )
    SELECT
        schema_version,
        evaluation_id,
        contact_id,
        account_id,
        instance_id,
        agent_id,
        evaluation_definition_title,
        evaluator,
        evaluation_definition_id,
        evaluation_definition_version,
        evaluation_start_timestamp,
        evaluation_submit_timestamp,
        evaluation_score_percentage,
        creator,
        auto_evaluated,
        resubmitted,
        evaluation_source,
        evaluation_type,
        calibration_session_id,
        evaluated_participant_id,
        evaluated_participant_role,
        evaluation_acknowledger_comment,
        evaluation_acknowledged_timestamp,
        evaluation_acknowledged_by_user_name,
        evaluation_acknowledged_by_user_id,
        metadata,
        sections,
        questions,
        source_file,
        processed_at,
        year,
        month,
        day
    FROM {staging_table}
    WHERE evaluation_id IS NOT NULL;

    COMMIT;
    """

    glue_context.write_dynamic_frame.from_options(
        frame=final_dyf,
        connection_type="redshift",
        connection_options={
            "redshiftTmpDir": redshift_tmp_dir,
            "aws_iam_role": args[
                "redshift_s3_role_arn"
            ],
            "useConnectionProperties": "true",
            "connectionName": args[
                "target_connection_name"
            ],
            "dbtable": staging_table,
            "preactions": preactions,
            "postactions": postactions,
            "extracopyoptions": (
                "ACCEPTINVCHARS "
                "TRUNCATECOLUMNS "
                "SERIALIZETOJSON"
            ),
        },
        transformation_ctx=(
            "AmazonRedshiftContactEvaluationsTarget"
        ),
    )

    valid_df.unpersist()
    prepared_df.unpersist()

    job.commit()

    print(
        "Contact Evaluations Redshift ETL "
        "completed successfully."
    )

    print(
        "===== CONTACT EVALUATIONS REDSHIFT ETL COMPLETED ====="
    )


if __name__ == "__main__":
    main()
