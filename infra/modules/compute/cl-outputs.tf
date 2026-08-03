# ============================================================
# CONTACT LENS GLUE SECURITY CONFIGURATION
# ============================================================

output "cl_glue_security_configuration_name" {
  description = "Name of the Contact Lens Glue security configuration"
  value       = aws_glue_security_configuration.cl_sec_config.name
}


# ============================================================
# CONTACT LENS NETWORK GLUE CONNECTION
# ============================================================

output "cl_network_connection_name" {
  description = "Name of the Contact Lens Glue NETWORK connection"
  value       = aws_glue_connection.cl_network_connection.name
}

output "cl_network_connection_id" {
  description = "ID of the Contact Lens Glue NETWORK connection"
  value       = aws_glue_connection.cl_network_connection.id
}


# ============================================================
# CONTACT LENS REDSHIFT JDBC GLUE CONNECTION
# ============================================================

output "cl_redshift_connection_name" {
  description = "Name of the Contact Lens Redshift JDBC Glue connection"
  value       = aws_glue_connection.cl_redshift_connection.name
}

output "cl_redshift_connection_id" {
  description = "ID of the Contact Lens Redshift JDBC Glue connection"
  value       = aws_glue_connection.cl_redshift_connection.id
}


# ============================================================
# CONTACT LENS PREPROCESS GLUE JOB
# ============================================================

output "cl_preprocess_job_name" {
  description = "Name of the Contact Lens preprocess Glue job"
  value       = aws_glue_job.cl_preprocess_job.name
}

output "cl_preprocess_job_arn" {
  description = "ARN of the Contact Lens preprocess Glue job"
  value       = aws_glue_job.cl_preprocess_job.arn
}


# ============================================================
# CONTACT LENS GLUE CRAWLER
# ============================================================

output "cl_crawler_name" {
  description = "Name of the Contact Lens Glue crawler"
  value       = aws_glue_crawler.cl_crawler.name
}

output "cl_crawler_arn" {
  description = "ARN of the Contact Lens Glue crawler"
  value       = aws_glue_crawler.cl_crawler.arn
}


# ============================================================
# CONTACT LENS REDSHIFT LOAD GLUE JOB
# ============================================================

output "cl_redshift_job_name" {
  description = "Name of the Contact Lens Redshift load Glue job"
  value       = aws_glue_job.cl_redshift_job.name
}

output "cl_redshift_job_arn" {
  description = "ARN of the Contact Lens Redshift load Glue job"
  value       = aws_glue_job.cl_redshift_job.arn
}


# ============================================================
# CONTACT LENS GLUE TRIGGERS
# ============================================================

output "cl_preprocess_schedule_trigger_name" {
  description = "Name of the Contact Lens scheduled preprocess trigger"
  value       = aws_glue_trigger.cl_preprocess_schedule_trigger.name
}

output "cl_crawler_trigger_name" {
  description = "Name of the Contact Lens preprocess-to-crawler trigger"
  value       = aws_glue_trigger.cl_crawler_trigger.name
}

output "cl_redshift_trigger_name" {
  description = "Name of the Contact Lens crawler-to-Redshift trigger"
  value       = aws_glue_trigger.cl_redshift_trigger.name
}


# ============================================================
# CONTACT LENS GLUE CONNECTION NETWORK DETAILS
# ============================================================

output "cl_glue_connection_subnet_id" {
  description = "Subnet used by the Contact Lens Glue connections"
  value       = var.glue_connection_subnet_id
}

output "cl_glue_connection_security_group_ids" {
  description = "Security groups used by the Contact Lens Glue connections"
  value       = var.glue_connection_security_group_ids
}

output "cl_glue_connection_availability_zone" {
  description = "Availability Zone used by the Contact Lens Glue connections"
  value       = var.glue_connection_availability_zone
}
