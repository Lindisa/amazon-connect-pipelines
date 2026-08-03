# ============================================================
# GLUE SECURITY CONFIGURATION
# ============================================================

resource "aws_glue_security_configuration" "cl_sec_config" {
  name = "${var.resource_prefix}-${var.environment}-afs1-cl-sec-config"

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
# CONTACT LENS NETWORK GLUE CONNECTION
#
# This connection is created and managed by the Contact Lens
# pipeline. The VPC is determined by the configured subnet.
# ============================================================

resource "aws_glue_connection" "cl_network_connection" {
  name            = var.network_glue_connection_name
  description     = "Glue network connection for ${var.environment} data pipelines"
  connection_type = "NETWORK"

  physical_connection_requirements {
    availability_zone      = var.glue_connection_availability_zone
    subnet_id              = var.glue_connection_subnet_id
    security_group_id_list = var.glue_connection_security_group_ids
  }

  tags = {
    Name        = var.network_glue_connection_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}


# ============================================================
# CONTACT LENS REDSHIFT JDBC GLUE CONNECTION
#
# This connection is created and managed by the Contact Lens
# pipeline using the configured subnet and security groups.
# ============================================================

resource "aws_glue_connection" "cl_redshift_connection" {
  name            = var.redshift_glue_connection_name
  description     = "Redshift JDBC connection for ${var.environment} data pipelines"
  connection_type = "JDBC"

  connection_properties = {
    JDBC_CONNECTION_URL = var.redshift_jdbc_url
    USERNAME            = var.redshift_username
    PASSWORD            = var.redshift_password
  }

  physical_connection_requirements {
    availability_zone      = var.glue_connection_availability_zone
    subnet_id              = var.glue_connection_subnet_id
    security_group_id_list = var.glue_connection_security_group_ids
  }

  tags = {
    Name        = var.redshift_glue_connection_name
    Environment = var.environment
    ManagedBy   = "Terraform"
  }
}


# ============================================================
# CONTACT LENS PREPROCESS GLUE JOB
# ============================================================

resource "aws_glue_job" "cl_preprocess_job" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-cl-pre-process"
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
    script_location = "s3://${var.scripts_bucket}/contact-lens/contact-lens-pre-process.py"
    python_version  = "3"
  }

  connections = [
    aws_glue_connection.cl_network_connection.name,
    aws_glue_connection.cl_redshift_connection.name
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
    "--target_prefix"      = "connect/absa-contact-centre-${var.environment}/ContactLens/pre-processed/"
    "--TempDir"            = "s3://${var.temp_bucket}/connect/absa-contact-centre-${var.environment}/ContactLens/temp/"
  }

  security_configuration = aws_glue_security_configuration.cl_sec_config.name

  depends_on = [
    aws_glue_connection.cl_network_connection,
    aws_glue_connection.cl_redshift_connection
  ]
}


# ============================================================
# CONTACT LENS GLUE CRAWLER
# ============================================================

resource "aws_glue_crawler" "cl_crawler" {
  name          = "${var.resource_prefix}-${var.environment}-afs1-cl-crawler"
  role          = var.glue_crawler_role_arn
  database_name = var.glue_catalog_database

  table_prefix = "contactlens_"

  s3_target {
    path       = "s3://${var.temp_bucket}/connect/absa-contact-centre-${var.environment}/ContactLens/pre-processed/"
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
# CONTACT LENS REDSHIFT LOAD GLUE JOB
# ============================================================

resource "aws_glue_job" "cl_redshift_job" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-cl-redshift-etl"
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
    script_location = "s3://${var.scripts_bucket}/contact-lens/load-contact-lens-to-redshift.py"
    python_version  = "3"
  }

  connections = [
    aws_glue_connection.cl_network_connection.name,
    aws_glue_connection.cl_redshift_connection.name
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
    "--target_connection_name" = aws_glue_connection.cl_redshift_connection.name
    "--job_store_bucket_name"  = "s3://${var.temp_bucket}/connect/absa-contact-centre-${var.environment}/ContactLens/redshift-temp/"
    "--redshift_s3_role_arn"    = var.redshift_role_arn
  }

  security_configuration = aws_glue_security_configuration.cl_sec_config.name

  depends_on = [
    aws_glue_connection.cl_network_connection,
    aws_glue_connection.cl_redshift_connection
  ]
}


# ============================================================
# TRIGGER: RUN CONTACT LENS PREPROCESS EVERY 30 MINUTES
# ============================================================

resource "aws_glue_trigger" "cl_preprocess_schedule_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-cl-preprocess-schedule"
  type              = "SCHEDULED"
  schedule          = "cron(0/30 * * * ? *)"
  start_on_creation = false

  actions {
    job_name = aws_glue_job.cl_preprocess_job.name
  }
}


# ============================================================
# TRIGGER: CONTACT LENS PREPROCESS -> CRAWLER
# ============================================================

resource "aws_glue_trigger" "cl_crawler_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-cl-crawler-trigger"
  type              = "CONDITIONAL"
  start_on_creation = false

  actions {
    crawler_name = aws_glue_crawler.cl_crawler.name
  }

  predicate {
    conditions {
      job_name = aws_glue_job.cl_preprocess_job.name
      state    = "SUCCEEDED"
    }
  }
}


# ============================================================
# TRIGGER: CONTACT LENS CRAWLER -> REDSHIFT LOAD
# ============================================================

resource "aws_glue_trigger" "cl_redshift_trigger" {
  name              = "${var.resource_prefix}-${var.environment}-afs1-cl-redshift-trigger"
  type              = "CONDITIONAL"
  start_on_creation = false

  actions {
    job_name = aws_glue_job.cl_redshift_job.name
  }

  predicate {
    conditions {
      crawler_name = aws_glue_crawler.cl_crawler.name
      crawl_state  = "SUCCEEDED"
    }
  }
}
