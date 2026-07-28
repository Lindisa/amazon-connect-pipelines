import sys
import json

from awsglue.transforms import *
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
from zoneinfo import zoneinfo


# ------------------------------------------------------------
# Read required Glue job parameters
# ------------------------------------------------------------
args = getResolvedOptions(sys.argv, [
    "JOB_NAME",
    "source_bucket_name",
    "source_prefix",
    "target_bucket_name",
    "target_prefix"
])


# ------------------------------------------------------------
# Build source and target S3 paths
# ------------------------------------------------------------
source_prefix = args["source_prefix"].strip("/")
target_prefix = args["target_prefix"].strip("/")

source_paths = []

# South African timezone
now = datetime.now(ZoneInfo("Africa/Johannesburg"))

# Today + previous 3 days

for i in range(4):
     d = now - timezone(days=i)

     source_paths.append(
          f"s3://{args['source_bucket_name']}/"
          f"{source_prefix}/"
          f"(d.strftime('%Y')}/"
          f"(d.strftime('%m')}/"
          f"(d.strftime('%d')}/"
     )

# source_ path = f"s3://{args['source_bucket_name']}/{source_prefix}/{current_year}/{current_month}/"
target_path    = f"s3://{args['source_bucket_name']}/{target_prefix}/

# ------------------------------------------------------------
# Initialise Glue and Spark context
# ------------------------------------------------------------
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args["JOB_NAME"], args)


print("===== CONTACT LENS PRE-PROCESS STARTED =====")
print(f"Job name: {args['JOB_NAME']}")
print(f"Target path: {target_path}")


# ------------------------------------------------------------
# 1. Read raw Contact Lens JSON files from S3.
#
# Exclusions must be supplied to Glue as a JSON-formatted
# string. The ** wildcard matches files in nested folders.
# ------------------------------------------------------------
AmazonS3Datasource = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    format="json",
    format_options={
        "multiline": False
    },
    connection_options={
        "paths": [source_path],
        "recurse": True,
        "groupFiles": "none",
        "exclusions": json.dumps([
            "**/contact-lens/pre-processed/**",
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


# ------------------------------------------------------------
# 2. Convert DynamicFrame to Spark DataFrame.
# ------------------------------------------------------------
df = AmazonS3Datasource.toDF()


# ------------------------------------------------------------
# Count the records read from the source.
#
# This counts JSON records, not S3 files.
# ------------------------------------------------------------
raw_count = df.count()

print(f"Raw records read from source S3 path: {raw_count}")


# ------------------------------------------------------------
# 3. Stop early if no data was read.
# ------------------------------------------------------------
if raw_count == 0:
    print("No new data to process.")
    job.commit()

else:

    # --------------------------------------------------------
    # 4. Add source_file using the actual S3 object path.
    # --------------------------------------------------------
    df = df.withColumn(
        "source_file",
        input_file_name()
    )


    # --------------------------------------------------------
    # 5. Extract year/month/day from the source path.
    #
    # Expected source pattern:
    # .../YYYY/MM/DD/file.json
    # --------------------------------------------------------
    date_path_regex = r"/(\d{4})/(\d{2})/(\d{2})/[^/]+\.json$"

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
    # 6. Identify records where partition values were not
    # extracted.
    # --------------------------------------------------------
    invalid_partition_condition = (
        (length(col("year")) == 0) |
        (length(col("month")) == 0) |
        (length(col("day")) == 0)
    )

    invalid_partition_df = df.filter(
        invalid_partition_condition
    )

    invalid_partition_count = invalid_partition_df.count()

    print(
        "Records with missing year/month/day partition values: "
        f"{invalid_partition_count}"
    )


    # --------------------------------------------------------
    # 7. Remove records with missing partition values.
    #
    # This prevents Glue from creating:
    # year=__HIVE_DEFAULT_PARTITION__
    # month=__HIVE_DEFAULT_PARTITION__
    # day=__HIVE_DEFAULT_PARTITION__
    # --------------------------------------------------------
    df = df.filter(
        (length(col("year")) > 0) &
        (length(col("month")) > 0) &
        (length(col("day")) > 0)
    )


    # --------------------------------------------------------
    # Log the number of valid records remaining after
    # partition filtering.
    # --------------------------------------------------------
    valid_partition_count = df.count()

    print(
        "Valid records remaining after partition filtering: "
        f"{valid_partition_count}"
    )


    # --------------------------------------------------------
    # 8. Convert complex columns into JSON strings before
    # writing to parquet.
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


    # --------------------------------------------------------
    # 9. Drop temporary source_file column.
    #
    # Redshift ETL extracts source_file later from
    # CustomerMetadata.InputS3Uri.
    # --------------------------------------------------------
    df = df.drop("source_file")


    # --------------------------------------------------------
    # 10. Show final count and sample before writing.
    # --------------------------------------------------------
    final_count = df.count()

    print(
        "Final records to write to pre-processed parquet: "
        f"{final_count}"
    )

    if final_count == 0:
        print(
            "No valid JSON records remain after "
            "partition filtering."
        )

        job.commit()

    else:
        # ----------------------------------------------------
        # 11. Convert back to DynamicFrame.
        # ----------------------------------------------------
        outputDf = DynamicFrame.fromDF(
            df,
            glueContext,
            "OutputDf"
        )


        # ----------------------------------------------------
        # 12. Write parquet files partitioned by year/month/day.
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


        print("Successfully wrote Contact Lens parquet files.")
        job.commit()


print("===== CONTACT LENS PRE-PROCESS COMPLETED =====")
