```hcl
# ============================================================
# CONTACT LENS GLUE S3 POLICY
# ============================================================

resource "aws_iam_policy" "cl_glue_s3_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-lens-glue-s3-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_s3_access.json
}


resource "aws_iam_role_policy_attachment" "cl_glue_preprocess_s3_attach" {
  role       = aws_iam_role.cl_glue_preprocess_role.name
  policy_arn = aws_iam_policy.cl_glue_s3_policy.arn
}


resource "aws_iam_role_policy_attachment" "cl_glue_redshift_s3_attach" {
  role       = aws_iam_role.cl_glue_redshift_role.name
  policy_arn = aws_iam_policy.cl_glue_s3_policy.arn
}


# ============================================================
# CONTACT LENS GLUE LOGS AND KMS POLICY
# ============================================================

resource "aws_iam_policy" "cl_glue_logs_kms_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-lens-glue-logs-kms-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_logs_kms_access.json
}


resource "aws_iam_role_policy_attachment" "cl_glue_preprocess_logs_kms_attach" {
  role       = aws_iam_role.cl_glue_preprocess_role.name
  policy_arn = aws_iam_policy.cl_glue_logs_kms_policy.arn
}


resource "aws_iam_role_policy_attachment" "cl_glue_redshift_logs_kms_attach" {
  role       = aws_iam_role.cl_glue_redshift_role.name
  policy_arn = aws_iam_policy.cl_glue_logs_kms_policy.arn
}


# ============================================================
# CONTACT LENS GLUE VPC POLICY
# ============================================================

resource "aws_iam_policy" "cl_glue_vpc_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-lens-glue-vpc-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_vpc_access.json
}


resource "aws_iam_role_policy_attachment" "cl_glue_preprocess_vpc_attach" {
  role       = aws_iam_role.cl_glue_preprocess_role.name
  policy_arn = aws_iam_policy.cl_glue_vpc_policy.arn
}


resource "aws_iam_role_policy_attachment" "cl_glue_redshift_vpc_attach" {
  role       = aws_iam_role.cl_glue_redshift_role.name
  policy_arn = aws_iam_policy.cl_glue_vpc_policy.arn
}


# ============================================================
# AWS MANAGED FULL-ACCESS POLICIES
# ============================================================

locals {
  cl_glue_full_access_managed_policies = {
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


resource "aws_iam_role_policy_attachment" "cl_glue_preprocess_full_access" {
  for_each = local.cl_glue_full_access_managed_policies

  role       = aws_iam_role.cl_glue_preprocess_role.name
  policy_arn = each.value
}


resource "aws_iam_role_policy_attachment" "cl_glue_redshift_full_access" {
  for_each = local.cl_glue_full_access_managed_policies

  role       = aws_iam_role.cl_glue_redshift_role.name
  policy_arn = each.value
}
```
