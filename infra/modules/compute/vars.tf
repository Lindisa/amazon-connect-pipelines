variable "resource_prefix" {
  type = string
}

variable "environment" {
  type = string
}

variable "glue_preprocess_role_arn" {
  type = string
}

variable "glue_redshift_role_arn" {
  type = string
}

variable "redshift_role_arn" {
  type = string
}

variable "glue_crawler_role_arn" {
  type = string
}

variable "glue_connection_name" {
  type = string
}

variable "glue_connection_availability_zone" {
  type = string
}

variable "glue_connection_subnet_id" {
  type = string
}

variable "glue_connection_name_security_group_id_list" {
  type = list(string)
}

variable "glue_catalog_database" {
  type = string
}

variable "glue_catalog_table" {
  type = string
}

variable "redshift_database" {
  type = string
}

variable "redshift_target_table" {
  type = string
}

variable "scripts_bucket" {
  type = string
}

variable "temp_bucket" {
  type = string
}

variable "source_prefix" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "s3_script_store_kms_key_arn" {
  type = string
}