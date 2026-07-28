```hcl
# ============================================================
# GLUE SECURITY CONFIGURATION
# ============================================================

resource "aws_glue_security_configuration" "ce_sec_config" {
  name = "${var.resource_prefix}-${var.environment}-afs1-ce-sec-config"

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
# ============================================================

data "aws_glue_connection" "network_connection" {
  id = "${data.aws_caller_identity.current.account_id}:${var.network_glue_connection_name}"
}


# ============================================================
# CONTACT EVALUATIONS PREPROCESS GLUE JOB
# ============================================================

resource "aws_glue_job" "ce_preprocess_job" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ce-pre-process"
  role_arn          = var.glue_preprocess_role_arn
  glue_version      = "5.0"
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
    script_location = "s3://${var.scripts_bucket}/contact-evaluations/contact-evaluations-pre-process.py"
    python_version  = "3"
  }

  connections = [
    data.aws_glue_connection.network_connection.name,
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

    "--source_bucket_name" = var.temp_bucket
    "--source_prefix"      = var.source_prefix
    "--target_bucket_name" = var.temp_bucket
    "--target_prefix"      = "connect/absa-contact-centre-${var.environment}/ContactEvaluations/pre-processed/"
    "--TempDir"            = "s3://${var.temp_bucket}/connect/absa-contact-centre-${var.environment}/ContactEvaluations/temp/"
  }

  security_configuration = aws_glue_security_configuration.ce_sec_config.name
}


# ============================================================
# CONTACT EVALUATIONS GLUE CRAWLER
# ============================================================

resource "aws_glue_crawler" "ce_crawler" {
  name          = "${var.resource_prefix}-${var.environment}-afs1-ce-crawler"
  role          = var.glue_crawler_role_arn
  database_name = var.glue_catalog_database

  table_prefix = "contactevaluations_"

  s3_target {
    path       = "s3://${var.temp_bucket}/connect/absa-contact-centre-${var.environment}/ContactEvaluations/pre-processed/"
    exclusions = ["**/temporary/**"]
  }

  schema_change_policy {
    delete_behavior = "LOG"
    update_behavior = "LOG"
  }

  configuration = jsonencode({
    Version = 1.0

    Grouping = {
      TableGroupingPolicy = "CombineCompatibleSchemas"
    }

    CreatePartitionIndex = true
  })

  recrawl_policy {
    recrawl_behavior = "CRAWL_NEW_FOLDERS_ONLY"
  }
}


# ============================================================
# CONTACT EVALUATIONS REDSHIFT LOAD GLUE JOB
# ============================================================

resource "aws_glue_job" "ce_redshift_job" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ce-redshift-etl"
  role_arn          = var.glue_redshift_role_arn
  glue_version      = "5.0"
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
    script_location = "s3://${var.scripts_bucket}/contact-evaluations/load-contact-evaluations-to-redshift.py"
    python_version  = "3"
  }

  connections = [
    data.aws_glue_connection.network_connection.name,
    data.aws_glue_connection.redshift_connection.name
  ]

  default_arguments = {
    "--enable-metrics"               = "true"
    "--enable-spark-ui"              = "true"
    "--enable-auto-scaling"          = "true"
    "--enable-job-insights"          = "true"
    "--enable-observability-metrics" = "true"
    "--enable-glue-datacatalog"      = "true"
    "--initial_load"                 = "false"
    "--job-bookmark-option"          = "job-bookmark-enable"
    "--job-language"                 = "python"

    "--source_database"        = var.glue_catalog_database
    "--source_table"           = var.glue_catalog_table
    "--target_table"           = var.redshift_target_table
    "--target_connection_name" = data.aws_glue_connection.redshift_connection.name
    "--job_store_bucket_name"  = "s3://${var.temp_bucket}/connect/absa-contact-centre-${var.environment}/ContactEvaluations/redshift-temp/"
    "--redshift_s3_role_arn"    = var.redshift_role_arn
  }

  security_configuration = aws_glue_security_configuration.ce_sec_config.name
}


# ============================================================
# TRIGGER: RUN PREPROCESS EVERY 30 MINUTES
# ============================================================

resource "aws_glue_trigger" "ce_preprocess_schedule_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ce-preprocess-schedule"
  type              = "SCHEDULED"
  schedule          = "cron(0/30 * * * ? *)"
  start_on_creation = false

  actions {
    job_name = aws_glue_job.ce_preprocess_job.name
  }
}


# ============================================================
# TRIGGER: PREPROCESS -> CRAWLER
# ============================================================

resource "aws_glue_trigger" "ce_crawler_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ce-crawler-trigger"
  type              = "CONDITIONAL"
  start_on_creation = false

  actions {
    crawler_name = aws_glue_crawler.ce_crawler.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.ce_preprocess_job.name
      state    = "SUCCEEDED"
    }
  }
}


# ============================================================
# TRIGGER: CRAWLER -> REDSHIFT LOAD
# ============================================================

resource "aws_glue_trigger" "ce_redshift_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-ce-redshift-trigger"
  type              = "CONDITIONAL"
  start_on_creation = false

  actions {
    job_name = aws_glue_job.ce_redshift_job.name
  }

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.ce_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }
}
```
