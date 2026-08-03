# ============================================================
# GENERAL
# ============================================================

variable "resource_prefix" {
  description = "Prefix used when naming Contact Lens AWS resources"
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
  description = "KMS key ARN used by the Glue security configuration"
  type        = string
}


# ============================================================
# S3
# ============================================================

variable "scripts_bucket" {
  description = "S3 bucket containing the Contact Lens Glue scripts"
  type        = string
}

variable "temp_bucket" {
  description = "S3 bucket used for Contact Lens source, processed and temporary data"
  type        = string
}

variable "source_prefix" {
  description = "S3 prefix containing the source Contact Lens data"
  type        = string
}


# ============================================================
# GLUE IAM ROLES
# ============================================================

variable "glue_preprocess_role_arn" {
  description = "IAM role ARN used by the Contact Lens preprocess Glue job"
  type        = string
}

variable "glue_redshift_role_arn" {
  description = "IAM role ARN used by the Contact Lens Redshift Glue job"
  type        = string
}

variable "glue_crawler_role_arn" {
  description = "IAM role ARN used by the Contact Lens Glue crawler"
  type        = string
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
# GLUE CONNECTION NAMES
# ============================================================

variable "network_glue_connection_name" {
  description = "Name of the Contact Lens Glue NETWORK connection"
  type        = string
}

variable "redshift_glue_connection_name" {
  description = "Name of the Contact Lens Redshift JDBC Glue connection"
  type        = string
}


# ============================================================
# GLUE NETWORK CONFIGURATION
#
# The VPC is determined by the subnet supplied here.
# ============================================================

variable "glue_connection_availability_zone" {
  description = "Availability Zone containing the subnet used by the Glue connections"
  type        = string
}

variable "glue_connection_subnet_id" {
  description = "Subnet ID used by the Contact Lens NETWORK and JDBC Glue connections"
  type        = string
}

variable "glue_connection_security_group_ids" {
  description = "Security group IDs assigned to the Contact Lens Glue connections"
  type        = list(string)
}


# ============================================================
# REDSHIFT JDBC CONNECTION
# ============================================================

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
# REDSHIFT LOAD
# ============================================================

variable "redshift_target_table" {
  description = "Redshift target table populated by the Contact Lens Glue job"
  type        = string
}

variable "redshift_role_arn" {
  description = "IAM role ARN used by Redshift to access Contact Lens data in S3"
  type        = string
}
