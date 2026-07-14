# GLOBAL

variable "environment" {
  type        = string
  description = "Deployment environment"
}

# S3 EXISTING BUCKETS

variable "source_bucket_name" {
  type        = string
  description = "Existing source bucket"
}

variable "target_bucket_name" {
  type        = string
  description = "Existing target bucket"
}

# GLUE CRAWLER

variable "glue_crawler_role_arn" {
  type        = string
  description = "IAM role ARN for Glue crawler"
}

variable "resource_prefix" {
  type = string
}