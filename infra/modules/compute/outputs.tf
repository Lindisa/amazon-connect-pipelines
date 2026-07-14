output "glue_connection_name" {
  value = var.glue_connection_name
}

output "preprocess_job_name" {
  value = aws_glue_job.preprocess_job.name
}

output "redshift_job_name" {
  value = aws_glue_job.redshift_job.name
}