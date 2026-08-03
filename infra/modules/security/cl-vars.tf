# ============================================================
# GENERAL
# ============================================================

variable "environment" {
  type = string
}

variable "resource_prefix" {
  type = string
}


# ============================================================
# S3
# ============================================================

variable "source_bucket_name" {
  type = string
}

variable "target_bucket_name" {
  type = string
}

variable "glue_scripts_bucket_name" {
  type = string
}


# ============================================================
# KMS
# ============================================================

variable "kms_key_arn" {
  type = string
}

variable "s3_script_store_kms_key_arn" {
  type = string
}


# ============================================================
# CONTACT LENS GLUE CONNECTIONS
# ============================================================

variable "network_glue_connection_name" {
  type = string
}

variable "redshift_glue_connection_name" {
  type = string
}

variable "glue_connection_availability_zone" {
  type = string
}

variable "glue_connection_subnet_id" {
  type = string
}

variable "glue_connection_security_group_ids" {
  type = list(string)
}

variable "redshift_jdbc_url" {
  type = string
}

variable "redshift_username" {
  type      = string
  sensitive = true
}

variable "redshift_password" {
  type      = string
  sensitive = true
}
