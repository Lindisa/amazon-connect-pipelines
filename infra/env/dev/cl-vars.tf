# ============================================================
# GLOBAL
# ============================================================

variable "aws_region" {
  description = "AWS region where the Contact Lens pipeline is deployed"
  type        = string
}

variable "environment" {
  description = "Deployment environment, for example dev, sit, uat or prod"
  type        = string
}

variable "project_id" {
  description = "Contact Lens pipeline project identifier"
  type        = string
}

variable "project_name" {
  description = "Contact Lens pipeline project name"
  type        = string
}

variable "resource_prefix" {
  description = "Prefix used when naming Contact Lens resources"
  type        = string
}


# ============================================================
# S3
# ============================================================

variable "source_bucket_name" {
  description = "S3 bucket containing the source Contact Lens data"
  type        = string
}

variable "source_prefix" {
  description = "S3 prefix containing the source Contact Lens data"
  type        = string
}

variable "target_bucket_name" {
  description = "S3 bucket containing processed Contact Lens data"
  type        = string
}

variable "target_prefix" {
  description = "S3 prefix used for the processed Contact Lens output"
  type        = string
}

variable "glue_scripts_bucket_name" {
  description = "S3 bucket containing the Contact Lens Glue scripts"
  type        = string
}


# ============================================================
# GLUE NETWORK CONNECTION
# ============================================================

variable "network_glue_connection_name" {
  description = "Name of the Contact Lens Glue NETWORK connection"
  type        = string
}

variable "glue_connection_availability_zone" {
  description = "Availability Zone containing the subnet used by the Glue connections"
  type        = string
}

variable "glue_connection_subnet_id" {
  description = "Subnet ID used by the Contact Lens Glue connections"
  type        = string
}

variable "glue_connection_security_group_ids" {
  description = "Security group IDs assigned to the Contact Lens Glue connections"
  type        = list(string)
}


# ============================================================
# REDSHIFT JDBC GLUE CONNECTION
# ============================================================

variable "redshift_glue_connection_name" {
  description = "Name of the Contact Lens Redshift JDBC Glue connection"
  type        = string
}

variable "redshift_jdbc_url" {
  description = "JDBC URL used by the Contact Lens Redshift Glue connection"
  type        = string
}

variable "redshift_username" {
  description = "Username used by the Contact Lens Redshift JDBC connection"
  type        = string
  sensitive   = true
}

variable "redshift_password" {
  description = "Password used by the Contact Lens Redshift JDBC connection"
  type        = string
  sensitive   = true
}


# ============================================================
# GLUE DATA CATALOG
# ============================================================

variable "glue_catalog_database" {
  description = "Glue Data Catalog database containing the Contact Lens table"
  type        = string
}

variable "glue_catalog_table" {
  description = "Glue Data Catalog table read by the Contact Lens Redshift job"
  type        = string
}


# ============================================================
# REDSHIFT
# ============================================================

variable "redshift_target_table" {
  description = "Redshift target table populated by the Contact Lens Glue job"
  type        = string
}

variable "redshift_role_arn" {
  description = "IAM role ARN used by Redshift to access Contact Lens data in S3"
  type        = string
}


# ============================================================
# KMS
# ============================================================

variable "kms_key_arn" {
  description = "KMS key ARN used by the Contact Lens Glue security configuration"
  type        = string
}

variable "s3_script_store_kms_key_arn" {
  description = "KMS key ARN used to encrypt the Glue script-store bucket"
  type        = string
}
