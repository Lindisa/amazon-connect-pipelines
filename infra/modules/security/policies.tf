resource "aws_iam_policy" "glue_s3_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-evaluations-glue-s3-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_s3_access.json
}

resource "aws_iam_role_policy_attachment" "glue_preprocess_attach" {
  role       = aws_iam_role.glue_preprocess_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_redshift_attach" {
  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}


resource "aws_iam_policy" "glue_logs_kms_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-evaluations-glue-logs-kms-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_logs_kms_access.json
}

resource "aws_iam_role_policy_attachment" "glue_preprocess_logs_kms" {
  role       = aws_iam_role.glue_preprocess_role.name
  policy_arn = aws_iam_policy.glue_logs_kms_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_redshift_logs_kms" {
  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = aws_iam_policy.glue_logs_kms_policy.arn
}


resource "aws_iam_policy" "glue_vpc_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-evaluations-glue-vpc-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_vpc_access.json
}

resource "aws_iam_role_policy_attachment" "glue_preprocess_vpc" {
  role       = aws_iam_role.glue_preprocess_role.name
  policy_arn = aws_iam_policy.glue_vpc_policy.arn
}

resource "aws_iam_role_policy_attachment" "glue_redshift_vpc" {
  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = aws_iam_policy.glue_vpc_policy.arn
}


locals {
  glue_full_access_managed_policies = {
    ec2_full                         = "arn:aws:iam::aws:policy/AmazonEC2FullAccess"
    s3_full                          = "arn:aws:iam::aws:policy/AmazonS3FullAccess"
    redshift_full                    = "arn:aws:iam::aws:policy/AmazonRedshiftFullAccess"
    redshift_all_commands_full       = "arn:aws:iam::aws:policy/AmazonRedshiftAllCommandsFullAccess"
    redshift_data_full               = "arn:aws:iam::aws:policy/AmazonRedshiftDataFullAccess"
    redshift_query_editor_full       = "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2FullAccess"
    redshift_query_editor_rw_sharing = "arn:aws:iam::aws:policy/AmazonRedshiftQueryEditorV2ReadWriteSharing"
    glue_console_full                = "arn:aws:iam::aws:policy/AWSGlueConsoleFullAccess"
  }
}

resource "aws_iam_role_policy_attachment" "glue_preprocess_full_access" {
  for_each = local.glue_full_access_managed_policies

  role       = aws_iam_role.glue_preprocess_role.name
  policy_arn = each.value
}

resource "aws_iam_role_policy_attachment" "glue_redshift_full_access" {
  for_each = local.glue_full_access_managed_policies

  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = each.value
}