output "glue_preprocess_role_arn" {
  value = aws_iam_role.glue_preprocess_role.arn
}

output "glue_redshift_role_arn" {
  value = aws_iam_role.glue_redshift_role.arn
}