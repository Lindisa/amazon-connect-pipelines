# ============================================================
# GENERAL VARIABLES
# ============================================================

variable "resource_prefix" {
  description = "Prefix used when naming AWS resources"
  type        = string
}

variable "environment" {
  description = "Deployment environment"
  type        = string
}


# ============================================================
# KMS
# ============================================================

variable "kms_key_arn" {
  description = "ARN of the KMS key used by the Glue security configuration and CloudWatch Logs"
  type        = string
}

variable "s3_script_store_kms_key_arn" {
  description = "ARN of the KMS key used to encrypt the Glue scripts bucket"
  type        = string
}


# ============================================================
# S3
# ============================================================

variable "source_bucket_name" {
  description = "S3 bucket containing the CTR source data referenced by the Glue Catalog table"
  type        = string
}

variable "glue_scripts_bucket_name" {
  description = "S3 bucket containing the CTR flattened Glue ETL script"
  type        = string
}

variable "target_bucket_name" {
  description = "S3 bucket used for temporary Redshift files"
  type        = string
}


# ============================================================
# EXISTING GLUE CONNECTIONS
# ============================================================

variable "glue_connection_name" {
  description = "Name of the existing enterprise Redshift JDBC Glue connection"
  type        = string
}

variable "network_glue_connection_name" {
  description = "Name of the existing shared network Glue connection"
  type        = string
}


# ============================================================
# EXISTING GLUE CATALOG
# ============================================================

variable "glue_catalog_database" {
  description = "Name of the existing Glue Catalog database containing the CTR source table"
  type        = string
}

variable "glue_catalog_table" {
  description = "Name of the existing Glue Catalog CTR source table"
  type        = string
}


# ============================================================
# REDSHIFT
# ============================================================

variable "redshift_target_table" {
  description = "Name of the CTR flattened Redshift target table"
  type        = string
}

variable "redshift_staging_table" {
  description = "Name of the CTR flattened Redshift staging table"
  type        = string
}

variable "redshift_role_arn" {
  description = "ARN of the IAM role used by Redshift to access temporary S3 files"
  type        = string
}
