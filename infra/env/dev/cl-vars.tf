# ============================================================
# GLOBAL
# ============================================================

variable "aws_region" {
  type = string
}

variable "environment" {
  type = string
}

variable "project_id" {
  type = string
}

variable "project_name" {
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

variable "source_prefix" {
  type = string
}

variable "target_bucket_name" {
  type = string
}

variable "target_prefix" {
  type = string
}

variable "glue_scripts_bucket_name" {
  type = string
}


# ============================================================
# GLUE CATALOG
# ============================================================

variable "glue_catalog_database" {
  type = string
}

variable "glue_catalog_table" {
  type = string
}


# ============================================================
# GLUE NETWORK CONNECTION
# ============================================================

variable "network_glue_connection_name" {
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


# ============================================================
# REDSHIFT JDBC GLUE CONNECTION
# ============================================================

variable "redshift_glue_connection_name" {
  type = string
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


# ============================================================
# IAM ROLES
# ============================================================

variable "glue_crawler_role_arn" {
  type = string
}

variable "redshift_role_arn" {
  type = string
}


# ============================================================
# REDSHIFT
# ============================================================

variable "redshift_target_table" {
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
