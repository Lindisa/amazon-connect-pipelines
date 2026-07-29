output "glue_connection_name" {
  value = var.glue_connection_name
}

output "preprocess_job_name" {
  value = aws_glue_job.ce_preprocess_job.name
}

output "redshift_job_name" {
  value = aws_glue_job.ce_redshift_job.name
}
