output "glue_connection_name" {
  value = var.glue_connection_name
}

output "network_glue_connection_name" {
  value = var.network_glue_connection_name
}

output "redshift_job_name" {
  value = aws_glue_job.ctr_flattened_redshift_job.name
}
