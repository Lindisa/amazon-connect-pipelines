import sys
import json

from awsglue.utils import getResolvedOptions
from awsglue.context import GlueContext
from awsglue.dynamicframe import DynamicFrame
from awsglue.job import Job

from pyspark.context import SparkContext
from pyspark.sql.functions import (
    col,
    input_file_name,
    regexp_extract,
    to_json,
    length
)
from pyspark.sql.types import StructType, ArrayType, MapType

from datetime import datetime, timedelta
from zoneinfo import ZoneInfo


# ------------------------------------------------------------
# Read required Glue job parameters
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_bucket_name",
    "source_prefix",
    "target_bucket_name",
    "target_prefix",
    "initial_load"
])


# ------------------------------------------------------------
# Build source and target S3 paths
# ------------------------------------------------------------
source_prefix = args["source_prefix"].strip("/")
target_prefix = args["target_prefix"].strip("/")

initial_load = (
    args["initial_load"].strip().lower() == "true"
)

source_paths = []


# ------------------------------------------------------------
# Initial load:
# Read the complete Contact Evaluations source prefix.
#
# Incremental load:
# Read today and the previous three days.
# ------------------------------------------------------------
if initial_load:
    source_paths.append(
        f"s3://{args['source_bucket_name']}/"
        f"{source_prefix}/"
    )

else:
    now = datetime.now(
        ZoneInfo("Africa/Johannesburg")
    )

    for i in range(4):
        processing_date = now - timedelta(days=i)

        source_paths.append(
            f"s3://{args['source_bucket_name']}/"
            f"{source_prefix}/"
            f"{processing_date.strftime('%Y')}/"
            f"{processing_date.strftime('%m')}/"
            f"{processing_date.strftime('%d')}/"
        )


target_path = (
    f"s3://{args['target_bucket_name']}/"
    f"{target_prefix}/"
)


# ------------------------------------------------------------
# Initialise Glue and Spark context
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


print("===== CONTACT EVALUATIONS PRE-PROCESS STARTED =====")
print(f"Job name: {args['JOB_NAME']}")
print(f"Initial load: {initial_load}")
print(f"Target path: {target_path}")


# ------------------------------------------------------------
# 1. Read raw Contact Evaluations JSON files from S3.
#
# Exclusions must be supplied to Glue as a JSON-formatted
# string. The ** wildcard matches nested folders.
# ------------------------------------------------------------
AmazonS3Datasource = (
    glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        format="json",
        format_options={
            "multiline": False
        },
        connection_options={
            "paths": source_paths,
            "recurse": True,
            "groupFiles": "none",
            "exclusions": json.dumps([
                "**/ContactEvaluations/pre-processed/**",
                "**/contact-evaluations/pre-processed/**",
                "**/pre-processed/**",
                "**/processing-failed/**",
                "**/redacted/**",
                "**/Redacted/**",
                "**/ivr/**",
                "**/IVR/**",
                "**/temporary/**",
                "**/Temporary/**",
                "**/__HIVE_DEFAULT_PARTITION__/**",
                "**/*.wav",
                "**/*.WAV",
                "**/*.mp3",
                "**/*.MP3",
                "**/*.flac",
                "**/*.FLAC",
                "**/*.parquet",
                "**/*.PARQUET",
                "**/_SUCCESS"
            ])
        },
        transformation_ctx="AmazonS3Datasource"
    )
)


# ------------------------------------------------------------
# 2. Convert the source DynamicFrame to a Spark DataFrame.
# ------------------------------------------------------------
df = AmazonS3Datasource.toDF()


# ------------------------------------------------------------
# Count JSON records read from the source.
# ------------------------------------------------------------
raw_count = df.count()

print(
    f"Raw Contact Evaluations records read: {raw_count}"
)


# ------------------------------------------------------------
# 3. Stop successfully when no records were read.
# ------------------------------------------------------------
if raw_count == 0:
    print("No Contact Evaluations records to process.")
    job.commit()

else:

    # --------------------------------------------------------
    # 4. Add and preserve the original source S3 JSON path.
    #
    # This column is retained in the Parquet output so that
    # every Redshift record can be traced back to its original
    # Contact Evaluation JSON file.
    # --------------------------------------------------------
    df = df.withColumn(
        "source_file",
        input_file_name()
    )


    # --------------------------------------------------------
    # 5. Extract year, month and day from the source path.
    #
    # Expected source pattern:
    # .../YYYY/MM/DD/file.json
    # --------------------------------------------------------
    date_path_regex = (
        r"/(\d{4})/(\d{2})/(\d{2})/[^/]+\.json$"
    )

    df = df.withColumn(
        "year",
        regexp_extract(
            col("source_file"),
            date_path_regex,
            1
        )
    )

    df = df.withColumn(
        "month",
        regexp_extract(
            col("source_file"),
            date_path_regex,
            2
        )
    )

    df = df.withColumn(
        "day",
        regexp_extract(
            col("source_file"),
            date_path_regex,
            3
        )
    )


    # --------------------------------------------------------
    # 6. Identify records where partition values could not be
    # extracted from the source path.
    # --------------------------------------------------------
    invalid_partition_condition = (
        (length(col("year")) == 0) |
        (length(col("month")) == 0) |
        (length(col("day")) == 0)
    )

    invalid_partition_count = (
        df
        .filter(invalid_partition_condition)
        .count()
    )

    print(
        "Records with missing year/month/day partition "
        f"values: {invalid_partition_count}"
    )


    # --------------------------------------------------------
    # 7. Remove records with missing partition values.
    #
    # This prevents Glue from creating default Hive
    # partition folders.
    # --------------------------------------------------------
    df = df.filter(
        (length(col("year")) > 0) &
        (length(col("month")) > 0) &
        (length(col("day")) > 0)
    )


    valid_partition_count = df.count()

    print(
        "Valid records remaining after partition filtering: "
        f"{valid_partition_count}"
    )


    # --------------------------------------------------------
    # 8. Convert complex columns into JSON strings before
    # writing to Parquet.
    #
    # source_file remains a normal string column and is
    # preserved in the output.
    # --------------------------------------------------------
    complex_columns = []

    for field in df.schema.fields:
        if isinstance(
            field.dataType,
            (StructType, ArrayType, MapType)
        ):
            complex_columns.append(field.name)

            df = df.withColumn(
                field.name,
                to_json(col(field.name))
            )


    print(
        "Complex columns converted to JSON strings: "
        f"{complex_columns}"
    )


    # --------------------------------------------------------
    # 9. Count final records before writing.
    # --------------------------------------------------------
    final_count = df.count()

    print(
        "Final Contact Evaluations records to write: "
        f"{final_count}"
    )


    if final_count == 0:
        print(
            "No valid Contact Evaluations records remain "
            "after partition filtering."
        )

        job.commit()

    else:

        # ----------------------------------------------------
        # 10. Convert the DataFrame back to a DynamicFrame.
        # ----------------------------------------------------
        outputDf = DynamicFrame.fromDF(
            df,
            glueContext,
            "OutputDf"
        )


        # ----------------------------------------------------
        # 11. Write Parquet files partitioned by:
        # year/month/day
        #
        # source_file is written as a regular Parquet column.
        # ----------------------------------------------------
        glueContext.write_dynamic_frame.from_options(
            frame=outputDf,
            connection_type="s3",
            connection_options={
                "path": target_path,
                "partitionKeys": [
                    "year",
                    "month",
                    "day"
                ]
            },
            format="parquet",
            transformation_ctx="S3Target"
        )


        print(
            "Successfully wrote Contact Evaluations "
            "pre-processed Parquet files."
        )

        job.commit()


print(
    "===== CONTACT EVALUATIONS PRE-PROCESS COMPLETED ====="
)
