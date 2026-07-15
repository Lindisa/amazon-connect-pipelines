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
    to_json,
    get_json_object,
    current_timestamp,
    trim,
    row_number,
    sha2,
    struct,
    to_timestamp
)
from pyspark.sql.types import (
    StructType,
    ArrayType,
    MapType,
    StringType,
    IntegerType,
    DoubleType,
    BooleanType,
    NullType,
    TimestampType
)


# ------------------------------------------------------------
# Exact Redshift target and staging column order
# ------------------------------------------------------------
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
    "day"
]


# ------------------------------------------------------------
# Columns stored as SUPER in Redshift
#
# The preprocess job converts these complex values into JSON
# strings before writing Parquet.
# ------------------------------------------------------------
SUPER_TARGET_COLUMNS = [
    "metadata",
    "sections",
    "questions"
]


# ------------------------------------------------------------
# Columns stored as VARCHAR in Redshift
# ------------------------------------------------------------
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
    "day"
]


# ------------------------------------------------------------
# Read an optional Glue job argument
# ------------------------------------------------------------
def get_optional_arg(arg_name, default_value):
    for index, argument in enumerate(sys.argv):
        if (
            argument == f"--{arg_name}"
            and index + 1 < len(sys.argv)
        ):
            return sys.argv[index + 1]

    return default_value


# ------------------------------------------------------------
# Build temporary S3 directory used by Redshift COPY
# ------------------------------------------------------------
def build_redshift_tmp_dir(bucket_value):
    value = (
        bucket_value
        .strip()
        .replace("s3://", "")
        .rstrip("/")
    )

    if "/" in value:
        return f"s3://{value}/"

    return f"s3://{value}/redshift_temp/"


# ------------------------------------------------------------
# Build a predicate containing every date in the lookback range
#
# lookback_days = 1:
#   yesterday and today
#
# lookback_days = 3:
#   three days ago through today
# ------------------------------------------------------------
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


# ------------------------------------------------------------
# Safely convert Contact Evaluation timestamp values.
#
# Valid examples:
# 2023-12-06T14:03:51Z
# 2023-12-06T14:03:51.969Z
# 2023-12-06 14:03:51
# 2023-12-06 14:03:51.969
# 2023-12-06T14:03:51+00:00
#
# Invalid values and timestamp format placeholders become NULL.
# ------------------------------------------------------------
def clean_timestamp(column_expression):
    cleaned_value = trim(
        column_expression.cast(StringType())
    )

    valid_timestamp_pattern = (
        r"^\d{4}-\d{2}-\d{2}"
        r"[T ]"
        r"\d{2}:\d{2}:\d{2}"
        r"(\.\d+)?"
        r"(Z|[+-]\d{2}:?\d{2})?$"
    )

    return when(
        cleaned_value.isNull()
        | (cleaned_value == "")
        | cleaned_value.contains("YYYY-MM-DD")
        | cleaned_value.startswith("[")
        | (~cleaned_value.rlike(valid_timestamp_pattern)),
        lit(None).cast(TimestampType())
    ).otherwise(
        to_timestamp(cleaned_value)
    )


# ------------------------------------------------------------
# Read a field from metadata.
#
# The preprocess job normally stores metadata as a JSON string,
# but StructType is also supported as a defensive fallback.
# ------------------------------------------------------------
def get_metadata_value(
    metadata_type,
    struct_field_name,
    json_path,
    target_type
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
            json_path
        ).cast(target_type)

    return lit(None).cast(target_type)


# ------------------------------------------------------------
# Extract metadata.score.percentage.
# ------------------------------------------------------------
def get_score_percentage(metadata_type):
    if isinstance(metadata_type, StringType):
        return get_json_object(
            col("metadata"),
            "$.score.percentage"
        ).cast(DoubleType())

    if isinstance(metadata_type, StructType):
        metadata_fields = {
            field.name: field.dataType
            for field in metadata_type.fields
        }

        if "score" not in metadata_fields:
            return lit(None).cast(DoubleType())

        score_type = metadata_fields["score"]

        if not isinstance(score_type, StructType):
            return lit(None).cast(DoubleType())

        score_fields = {
            field.name
            for field in score_type.fields
        }

        if "percentage" not in score_fields:
            return lit(None).cast(DoubleType())

        return col(
            "metadata.score.percentage"
        ).cast(DoubleType())

    return lit(None).cast(DoubleType())


# ------------------------------------------------------------
# Normalise and transform Contact Evaluation source data
# ------------------------------------------------------------
def transform_source_dataframe(source_df):
    spark_df = source_df


    # --------------------------------------------------------
    # 1. Convert top-level source column names to lowercase
    # --------------------------------------------------------
    original_columns = list(spark_df.columns)

    lowercase_columns = [
        column_name.lower()
        for column_name in original_columns
    ]

    if len(lowercase_columns) != len(
        set(lowercase_columns)
    ):
        raise ValueError(
            "Lowercasing the source column names would create "
            "duplicate column names."
        )

    for column_name in original_columns:
        lowercase_name = column_name.lower()

        if column_name != lowercase_name:
            spark_df = spark_df.withColumnRenamed(
                column_name,
                lowercase_name
            )


    # --------------------------------------------------------
    # 2. Rename top-level Contact Evaluation fields
    # --------------------------------------------------------
    if "schemaversion" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(
            "schemaversion",
            "schema_version"
        )

    elif "schema_version" not in spark_df.columns:
        spark_df = spark_df.withColumn(
            "schema_version",
            lit(None).cast(StringType())
        )


    if "evaluationid" in spark_df.columns:
        spark_df = spark_df.withColumnRenamed(
            "evaluationid",
            "evaluation_id"
        )

    elif "evaluation_id" not in spark_df.columns:
        spark_df = spark_df.withColumn(
            "evaluation_id",
            lit(None).cast(StringType())
        )


    # --------------------------------------------------------
    # 3. Extract Contact Evaluation metadata fields
    #
    # Timestamp fields are initially extracted as strings.
    # They are cleaned and converted separately.
    # --------------------------------------------------------
    if "metadata" in spark_df.columns:
        metadata_type = (
            spark_df.schema["metadata"].dataType
        )

        metadata_fields = [
            (
                "contact_id",
                "contactId",
                "$.contactId",
                StringType()
            ),
            (
                "account_id",
                "accountId",
                "$.accountId",
                StringType()
            ),
            (
                "instance_id",
                "instanceId",
                "$.instanceId",
                StringType()
            ),
            (
                "agent_id",
                "agentId",
                "$.agentId",
                StringType()
            ),
            (
                "evaluation_definition_title",
                "evaluationDefinitionTitle",
                "$.evaluationDefinitionTitle",
                StringType()
            ),
            (
                "evaluator",
                "evaluator",
                "$.evaluator",
                StringType()
            ),
            (
                "evaluation_definition_id",
                "evaluationDefinitionId",
                "$.evaluationDefinitionId",
                StringType()
            ),
            (
                "evaluation_definition_version",
                "evaluationDefinitionVersion",
                "$.evaluationDefinitionVersion",
                IntegerType()
            ),
            (
                "evaluation_start_timestamp",
                "evaluationStartTimestamp",
                "$.evaluationStartTimestamp",
                StringType()
            ),
            (
                "evaluation_submit_timestamp",
                "evaluationSubmitTimestamp",
                "$.evaluationSubmitTimestamp",
                StringType()
            ),
            (
                "creator",
                "creator",
                "$.creator",
                StringType()
            ),
            (
                "auto_evaluated",
                "autoEvaluated",
                "$.autoEvaluated",
                BooleanType()
            ),
            (
                "resubmitted",
                "resubmitted",
                "$.resubmitted",
                BooleanType()
            ),
            (
                "evaluation_source",
                "evaluationSource",
                "$.evaluationSource",
                StringType()
            ),
            (
                "evaluation_type",
                "evaluationType",
                "$.evaluationType",
                StringType()
            ),
            (
                "calibration_session_id",
                "calibrationSessionId",
                "$.calibrationSessionId",
                StringType()
            ),
            (
                "evaluated_participant_id",
                "evaluatedParticipantId",
                "$.evaluatedParticipantId",
                StringType()
            ),
            (
                "evaluated_participant_role",
                "evaluatedParticipantRole",
                "$.evaluatedParticipantRole",
                StringType()
            ),
            (
                "evaluation_acknowledger_comment",
                "evaluationAcknowledgerComment",
                "$.evaluationAcknowledgerComment",
                StringType()
            ),
            (
                "evaluation_acknowledged_timestamp",
                "evaluationAcknowledgedTimestamp",
                "$.evaluationAcknowledgedTimestamp",
                StringType()
            ),
            (
                "evaluation_acknowledged_by_user_name",
                "evaluationAcknowledgedByUserName",
                "$.evaluationAcknowledgedByUserName",
                StringType()
            ),
            (
                "evaluation_acknowledged_by_user_id",
                "evaluationAcknowledgedByUserId",
                "$.evaluationAcknowledgedByUserId",
                StringType()
            )
        ]

        for (
            target_column,
            struct_field_name,
            json_path,
            target_type
        ) in metadata_fields:
            spark_df = spark_df.withColumn(
                target_column,
                get_metadata_value(
                    metadata_type,
                    struct_field_name,
                    json_path,
                    target_type
                )
            )

        spark_df = spark_df.withColumn(
            "evaluation_score_percentage",
            get_score_percentage(
                metadata_type
            )
        )


        # ----------------------------------------------------
        # Clean and convert timestamp fields
        # ----------------------------------------------------
        spark_df = spark_df.withColumn(
            "evaluation_start_timestamp",
            clean_timestamp(
                col("evaluation_start_timestamp")
            )
        )

        spark_df = spark_df.withColumn(
            "evaluation_submit_timestamp",
            clean_timestamp(
                col("evaluation_submit_timestamp")
            )
        )

        spark_df = spark_df.withColumn(
            "evaluation_acknowledged_timestamp",
            clean_timestamp(
                col("evaluation_acknowledged_timestamp")
            )
        )

    else:
        print(
            "WARNING: metadata is missing from the source."
        )

        missing_string_columns = [
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
            "evaluation_acknowledged_by_user_id"
        ]

        for column_name in missing_string_columns:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType())
            )

        spark_df = spark_df.withColumn(
            "evaluation_definition_version",
            lit(None).cast(IntegerType())
        )

        spark_df = spark_df.withColumn(
            "evaluation_start_timestamp",
            lit(None).cast(TimestampType())
        )

        spark_df = spark_df.withColumn(
            "evaluation_submit_timestamp",
            lit(None).cast(TimestampType())
        )

        spark_df = spark_df.withColumn(
            "evaluation_score_percentage",
            lit(None).cast(DoubleType())
        )

        spark_df = spark_df.withColumn(
            "auto_evaluated",
            lit(None).cast(BooleanType())
        )

        spark_df = spark_df.withColumn(
            "resubmitted",
            lit(None).cast(BooleanType())
        )

        spark_df = spark_df.withColumn(
            "evaluation_acknowledged_timestamp",
            lit(None).cast(TimestampType())
        )


    # --------------------------------------------------------
    # 4. Preserve source_file from the preprocess job
    # --------------------------------------------------------
    if "source_file" not in spark_df.columns:
        spark_df = spark_df.withColumn(
            "source_file",
            lit(None).cast(StringType())
        )


    # --------------------------------------------------------
    # 5. Add the ETL processing timestamp
    # --------------------------------------------------------
    spark_df = spark_df.withColumn(
        "processed_at",
        current_timestamp()
    )


    # --------------------------------------------------------
    # 6. Ensure complex fields are JSON strings
    #
    # The preprocess job normally already performs this.
    # Structs, arrays and maps are handled again only as a
    # defensive fallback.
    # --------------------------------------------------------
    schema_types = {
        field.name: field.dataType
        for field in spark_df.schema.fields
    }

    for column_name in SUPER_TARGET_COLUMNS:
        if column_name not in spark_df.columns:
            continue

        column_type = schema_types[column_name]

        if isinstance(column_type, NullType):
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType())
            )

        elif isinstance(
            column_type,
            (StructType, ArrayType, MapType)
        ):
            spark_df = spark_df.withColumn(
                column_name,
                when(
                    col(column_name).isNull(),
                    lit(None).cast(StringType())
                ).otherwise(
                    to_json(col(column_name))
                )
            )

        elif isinstance(column_type, StringType):
            spark_df = spark_df.withColumn(
                column_name,
                when(
                    col(column_name).isNull()
                    | (trim(col(column_name)) == ""),
                    lit(None).cast(StringType())
                ).otherwise(
                    col(column_name).cast(StringType())
                )
            )

        else:
            print(
                f"WARNING: {column_name} has unsupported type "
                f"{column_type}. Its value will be stored as NULL."
            )

            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType())
            )

    return spark_df


# ------------------------------------------------------------
# Add missing columns and enforce required Spark data types
# ------------------------------------------------------------
def align_target_schema(spark_df):
    existing_columns = set(
        spark_df.columns
    )


    # --------------------------------------------------------
    # 7. Add target columns missing from the source
    # --------------------------------------------------------
    for column_name in ALL_TARGET_COLUMNS:
        if column_name in existing_columns:
            continue

        if column_name == "processed_at":
            spark_df = spark_df.withColumn(
                column_name,
                current_timestamp()
            )

        elif column_name == "evaluation_definition_version":
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(IntegerType())
            )

        elif column_name == "evaluation_score_percentage":
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(DoubleType())
            )

        elif column_name in [
            "auto_evaluated",
            "resubmitted"
        ]:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(BooleanType())
            )

        elif column_name in [
            "evaluation_start_timestamp",
            "evaluation_submit_timestamp",
            "evaluation_acknowledged_timestamp"
        ]:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(TimestampType())
            )

        else:
            spark_df = spark_df.withColumn(
                column_name,
                lit(None).cast(StringType())
            )


    # --------------------------------------------------------
    # 8. Ensure SUPER input columns are JSON strings
    # --------------------------------------------------------
    for column_name in SUPER_TARGET_COLUMNS:
        spark_df = spark_df.withColumn(
            column_name,
            col(column_name).cast(StringType())
        )


    # --------------------------------------------------------
    # 9. Ensure Redshift VARCHAR fields are Spark strings
    # --------------------------------------------------------
    for column_name in STRING_TARGET_COLUMNS:
        spark_df = spark_df.withColumn(
            column_name,
            col(column_name).cast(StringType())
        )


    # --------------------------------------------------------
    # 10. Ensure numeric and Boolean fields use exact types
    # --------------------------------------------------------
    spark_df = spark_df.withColumn(
        "evaluation_definition_version",
        col(
            "evaluation_definition_version"
        ).cast(IntegerType())
    )

    spark_df = spark_df.withColumn(
        "evaluation_score_percentage",
        col(
            "evaluation_score_percentage"
        ).cast(DoubleType())
    )

    spark_df = spark_df.withColumn(
        "auto_evaluated",
        col("auto_evaluated").cast(BooleanType())
    )

    spark_df = spark_df.withColumn(
        "resubmitted",
        col("resubmitted").cast(BooleanType())
    )


    # --------------------------------------------------------
    # 11. Ensure timestamp fields use exact timestamp types
    # --------------------------------------------------------
    timestamp_columns = [
        "evaluation_start_timestamp",
        "evaluation_submit_timestamp",
        "evaluation_acknowledged_timestamp",
        "processed_at"
    ]

    for column_name in timestamp_columns:
        spark_df = spark_df.withColumn(
            column_name,
            col(column_name).cast(TimestampType())
        )


    # --------------------------------------------------------
    # 12. Clean EvaluationId once
    # --------------------------------------------------------
    spark_df = spark_df.withColumn(
        "evaluation_id",
        when(
            col("evaluation_id").isNull()
            | (trim(col("evaluation_id")) == ""),
            lit(None).cast(StringType())
        ).otherwise(
            trim(col("evaluation_id"))
        )
    )

    return spark_df


# ------------------------------------------------------------
# Deterministically keep one row per EvaluationId in this run
# ------------------------------------------------------------
def remove_duplicate_evaluation_ids(valid_df):
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
            256
        )
    )


    # --------------------------------------------------------
    # Duplicate preference:
    #
    # 1. Latest evaluation submit timestamp
    # 2. Latest evaluation start timestamp
    # 3. Latest year
    # 4. Latest month
    # 5. Latest day
    # 6. Highest source_file
    # 7. Stable row hash
    # --------------------------------------------------------
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
            col("__row_hash").desc_nulls_last()
        )
    )

    return (
        valid_df
        .withColumn(
            "__duplicate_row_number",
            row_number().over(
                duplicate_window
            )
        )
        .filter(
            col("__duplicate_row_number") == 1
        )
        .drop(
            "__duplicate_row_number",
            "__row_hash"
        )
    )


# ------------------------------------------------------------
# Main Glue job
# ------------------------------------------------------------
def main():
    # --------------------------------------------------------
    # Read required job parameters
    # --------------------------------------------------------
    args = getResolvedOptions(
        sys.argv,
        [
            "JOB_NAME",
            "source_table",
            "source_database",
            "target_table",
            "target_connection_name",
            "job_store_bucket_name",
            "redshift_s3_role_arn"
        ]
    )


    # --------------------------------------------------------
    # Initialise Glue and Spark
    # --------------------------------------------------------
    spark_context = SparkContext()

    glue_context = GlueContext(
        spark_context
    )

    job = Job(glue_context)

    job.init(
        args["JOB_NAME"],
        args
    )


    # --------------------------------------------------------
    # Job configuration
    # --------------------------------------------------------
    target_table = args["target_table"]

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
                "1"
            )
        )
    except ValueError as error:
        raise ValueError(
            "lookback_days must be a valid integer."
        ) from error

    initial_load = (
        get_optional_arg(
            "initial_load",
            "false"
        ).strip().lower()
        == "true"
    )


    # --------------------------------------------------------
    # Read Contact Evaluations data from the Glue Catalog
    # --------------------------------------------------------
    if initial_load:
        print(
            "Performing full Contact Evaluations Redshift load."
        )

        source_dyf = (
            glue_context
            .create_dynamic_frame
            .from_catalog(
                database=args["source_database"],
                table_name=args["source_table"],
                transformation_ctx="source_dyf"
            )
        )

    else:
        predicate = build_partition_predicate(
            lookback_days
        )

        print(
            "Performing incremental Contact Evaluations "
            "Redshift load."
        )

        print(
            f"Predicate: {predicate}"
        )

        source_dyf = (
            glue_context
            .create_dynamic_frame
            .from_catalog(
                database=args["source_database"],
                table_name=args["source_table"],
                push_down_predicate=predicate,
                transformation_ctx="source_dyf"
            )
        )


    # --------------------------------------------------------
    # Convert source DynamicFrame to Spark DataFrame
    # --------------------------------------------------------
    source_df = source_dyf.toDF()

    source_record_count = source_df.count()

    print(
        "Source Contact Evaluation records read from "
        f"Glue Catalog: {source_record_count}"
    )


    # --------------------------------------------------------
    # Successful no-source-record completion
    # --------------------------------------------------------
    if source_record_count == 0:
        print(
            "No source records were found for the selected "
            "partitions. Redshift will not be updated."
        )

        job.commit()

        print(
            "Contact Evaluations Redshift ETL completed "
            "successfully with no source records."
        )

        return


    # --------------------------------------------------------
    # Transform and align source data
    # --------------------------------------------------------
    transformed_df = transform_source_dataframe(
        source_df
    )

    prepared_df = align_target_schema(
        transformed_df
    ).cache()


    # --------------------------------------------------------
    # Count invalid timestamp rows for visibility
    # --------------------------------------------------------
    print(
        "Rows with NULL evaluation_start_timestamp: "
        f"{prepared_df.filter(col('evaluation_start_timestamp').isNull()).count()}"
    )

    print(
        "Rows with NULL evaluation_submit_timestamp: "
        f"{prepared_df.filter(col('evaluation_submit_timestamp').isNull()).count()}"
    )

    print(
        "Rows with NULL evaluation_acknowledged_timestamp: "
        f"{prepared_df.filter(col('evaluation_acknowledged_timestamp').isNull()).count()}"
    )


    # --------------------------------------------------------
    # Count rows removed because EvaluationId is missing
    # --------------------------------------------------------
    missing_evaluation_id_count = (
        prepared_df
        .filter(
            col("evaluation_id").isNull()
        )
        .count()
    )

    print(
        "Records removed because evaluation_id was missing "
        f"or blank: {missing_evaluation_id_count}"
    )


    # --------------------------------------------------------
    # Keep only records with a valid EvaluationId
    # --------------------------------------------------------
    valid_df = prepared_df.filter(
        col("evaluation_id").isNotNull()
    )


    # --------------------------------------------------------
    # Remove duplicate EvaluationIds within this job run
    # --------------------------------------------------------
    valid_df = remove_duplicate_evaluation_ids(
        valid_df
    )


    # --------------------------------------------------------
    # Enforce exact target column order and cache final data
    # --------------------------------------------------------
    valid_df = (
        valid_df
        .select(
            *ALL_TARGET_COLUMNS
        )
        .cache()
    )

    valid_record_count = valid_df.count()

    print(
        "Valid deduplicated evaluations to load into staging: "
        f"{valid_record_count}"
    )


    # --------------------------------------------------------
    # Successful no-valid-record completion
    # --------------------------------------------------------
    if valid_record_count == 0:
        print(
            "No valid records remain after EvaluationId "
            "validation. Redshift will not be updated."
        )

        valid_df.unpersist()
        prepared_df.unpersist()

        job.commit()

        print(
            "Contact Evaluations Redshift ETL completed "
            "successfully with no valid records."
        )

        return


    # --------------------------------------------------------
    # Show sample data in Glue logs
    # --------------------------------------------------------
    print(
        "Sample before Redshift staging write:"
    )

    valid_df.select(
        "evaluation_id",
        "contact_id",
        "evaluation_definition_title",
        "evaluation_start_timestamp",
        "evaluation_submit_timestamp",
        "evaluation_acknowledged_timestamp",
        "evaluation_score_percentage",
        "source_file",
        "year",
        "month",
        "day"
    ).show(
        10,
        truncate=False
    )


    # --------------------------------------------------------
    # Convert final DataFrame back to DynamicFrame
    # --------------------------------------------------------
    final_dyf = DynamicFrame.fromDF(
        valid_df,
        glue_context,
        "final_redshift_frame"
    )


    # --------------------------------------------------------
    # Clear only the staging table before the batch load
    # --------------------------------------------------------
    preactions = f"""
    TRUNCATE TABLE {staging_table};
    """


    # --------------------------------------------------------
    # Insert only EvaluationIds not already in target
    # --------------------------------------------------------
    postactions = f"""
    BEGIN;

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
        s.schema_version,
        s.evaluation_id,
        s.contact_id,
        s.account_id,
        s.instance_id,
        s.agent_id,
        s.evaluation_definition_title,
        s.evaluator,
        s.evaluation_definition_id,
        s.evaluation_definition_version,
        s.evaluation_start_timestamp,
        s.evaluation_submit_timestamp,
        s.evaluation_score_percentage,
        s.creator,
        s.auto_evaluated,
        s.resubmitted,
        s.evaluation_source,
        s.evaluation_type,
        s.calibration_session_id,
        s.evaluated_participant_id,
        s.evaluated_participant_role,
        s.evaluation_acknowledger_comment,
        s.evaluation_acknowledged_timestamp,
        s.evaluation_acknowledged_by_user_name,
        s.evaluation_acknowledged_by_user_id,
        s.metadata,
        s.sections,
        s.questions,
        s.source_file,
        s.processed_at,
        s.year,
        s.month,
        s.day
    FROM {staging_table} AS s
    WHERE s.evaluation_id IS NOT NULL
      AND NOT EXISTS (
          SELECT 1
          FROM {target_table} AS t
          WHERE t.evaluation_id = s.evaluation_id
      );

    COMMIT;
    """


    # --------------------------------------------------------
    # Write current batch to Redshift staging
    #
    # Same working Contact Lens write pattern:
    # - matching target and staging schemas
    # - SUPER columns in both tables
    # - no TRUNCATECOLUMNS
    # - no SERIALIZETOJSON
    # --------------------------------------------------------
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
            "extracopyoptions": "ACCEPTINVCHARS"
        },
        transformation_ctx="AmazonRedshiftTarget"
    )


    # --------------------------------------------------------
    # Release cached Spark data
    # --------------------------------------------------------
    valid_df.unpersist()
    prepared_df.unpersist()


    # --------------------------------------------------------
    # Complete Glue job
    # --------------------------------------------------------
    job.commit()

    print(
        "Contact Evaluations Redshift ETL completed "
        "successfully."
    )


# ------------------------------------------------------------
# Run the Glue job
# ------------------------------------------------------------
if __name__ == "__main__":
    main()
