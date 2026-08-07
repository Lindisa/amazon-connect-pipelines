# ============================================================
# GENERAL VARIABLES
# ============================================================

variable "resource_prefix" {
  description = "Prefix used when naming AWS resources"
  type        = string
}

variable "environment" {
  description = "Deployment environment, for example dev, sit, uat or prod"
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
  description = "ARN of the KMS key used to encrypt the Glue script S3 bucket"
  type        = string
}


# ============================================================
# S3
# ============================================================

variable "source_bucket_name" {
  description = "S3 bucket containing the CTR data referenced by the existing Glue Catalog table"
  type        = string
}

variable "scripts_bucket" {
  description = "S3 bucket containing the CTR flattened Glue ETL script"
  type        = string
}

variable "temp_bucket" {
  description = "S3 bucket used by the Glue job for temporary Redshift files"
  type        = string
}
