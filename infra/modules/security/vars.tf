variable "environment" {
  type = string
}

variable "source_bucket_name" {
  type = string
}

variable "target_bucket_name" {
  type = string
}

variable "glue_scripts_bucket_name" {
  type = string
}

variable "kms_key_arn" {
  type = string
}

variable "resource_prefix" {
  type = string
}

variable "s3_script_store_kms_key_arn" {
  type = string
}