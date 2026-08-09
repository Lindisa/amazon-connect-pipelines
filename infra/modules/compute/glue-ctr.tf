# ============================================================
# GLUE SECURITY CONFIGURATION
# ============================================================

resource "aws_glue_security_configuration" "ctr_flattened_sec_config" {
  name = "${var.resource_prefix}-${var.environment}-afs1-ctr-flattened-sec-config"

  encryption_configuration {
    cloudwatch_encryption {
      cloudwatch_encryption_mode = "SSE-KMS"
      kms_key_arn                = var.kms_key_arn
    }

    job_bookmarks_encryption {
      job_bookmarks_encryption_mode = "CSE-KMS"
      kms_key_arn                   = var.kms_key_arn
    }

    s3_encryption {
      s3_encryption_mode = "SSE-KMS"
      kms_key_arn        = var.kms_key_arn
    }
  }
}


# ============================================================
# CURRENT AWS ACCOUNT
# ============================================================

data "aws_caller_identity" "current" {}


# ============================================================
# EXISTING ENTERPRISE REDSHIFT/JDBC GLUE CONNECTION
# ============================================================

data "aws_glue_connection" "redshift_connection" {
  id = "${data.aws_caller_identity.current.account_id}:${var.glue_connection_name}"
}


# ============================================================
# EXISTING SHARED NETWORK GLUE CONNECTION
#
# This connection is imported into the CTR Terraform state by
# the deployment workflow before Terraform creates the job.
#
# prevent_destroy protects the shared connection from deletion.
# ignore_changes prevents this pipeline from modifying the
# connection configuration owned by another pipeline.
# ============================================================

resource "aws_glue_connection" "network_connection" {
  name            = var.network_glue_connection_name
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone      = var.glue_connection_availability_zone
    subnet_id              = var.glue_connection_subnet_id
    security_group_id_list = var.glue_connection_security_group_ids
  }

  lifecycle {
    prevent_destroy = true

    ignore_changes = [
      connection_type,
      description,
      match_criteria,
      physical_connection_requirements,
      tags,
      tags_all
    ]
  }
}


# ============================================================
# CTR FLATTENED REDSHIFT ETL GLUE JOB
# ============================================================

resource "aws_glue_job" "ctr_flattened_redshift_job" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ctr-flattened-redshift-etl"
  role_arn          = var.glue_redshift_role_arn
  glue_version      = "5.1"
  max_retries       = 0
  timeout           = 2880
  number_of_workers = 2
  worker_type       = "G.1X"
  execution_class   = "STANDARD"

  execution_property {
    max_concurrent_runs = 1
  }

  command {
    name            = "glueetl"
    script_location = "s3://${var.scripts_bucket}/ctr-flattened/load-ctr-flattened-to-redshift.py"
    python_version  = "3"
  }

  connections = [
    aws_glue_connection.network_connection.name,
    data.aws_glue_connection.redshift_connection.name
  ]

  default_arguments = {
    "--enable-metrics"               = "true"
    "--enable-spark-ui"              = "true"
    "--enable-auto-scaling"          = "true"
    "--enable-job-insights"          = "true"
    "--enable-observability-metrics" = "true"
    "--enable-glue-datacatalog"      = "true"
    "--job-bookmark-option"          = "job-bookmark-enable"
    "--job-language"                 = "python"
    "--initial_load"                 = "false"

    "--source_database"        = var.glue_catalog_database
    "--source_table"           = var.glue_catalog_table
    "--target_table"           = var.redshift_target_table
    "--staging_table"          = var.redshift_staging_table
    "--target_connection_name" = data.aws_glue_connection.redshift_connection.name
    "--job_store_bucket_name"  = "s3://${var.temp_bucket}/connect/${var.environment}/ctr-flattened/redshift-temp/"
    "--redshift_s3_role_arn"    = var.redshift_role_arn
  }

  security_configuration = aws_glue_security_configuration.ctr_flattened_sec_config.name

  depends_on = [
    aws_glue_connection.network_connection
  ]
}


# ============================================================
# TRIGGER: RUN CTR FLATTENED JOB EVERY 30 MINUTES
# ============================================================

resource "aws_glue_trigger" "ctr_flattened_schedule_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ctr-flattened-schedule"
  type              = "SCHEDULED"
  schedule          = "cron(0/30 * * * ? *)"
  start_on_creation = false

  actions {
    job_name = aws_glue_job.ctr_flattened_redshift_job.name
  }
}
