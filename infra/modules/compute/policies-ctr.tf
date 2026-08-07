# ============================================================
# GLUE S3 ACCESS POLICY
# ============================================================

resource "aws_iam_policy" "glue_s3_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-ctr-flattened-glue-s3-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_s3_access.json
}

resource "aws_iam_role_policy_attachment" "glue_redshift_s3_attach" {
  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = aws_iam_policy.glue_s3_policy.arn
}


# ============================================================
# GLUE LOGS AND KMS ACCESS POLICY
# ============================================================

resource "aws_iam_policy" "glue_logs_kms_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-ctr-flattened-glue-logs-kms-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_logs_kms_access.json
}

resource "aws_iam_role_policy_attachment" "glue_redshift_logs_kms_attach" {
  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = aws_iam_policy.glue_logs_kms_policy.arn
}


# ============================================================
# GLUE VPC ACCESS POLICY
# ============================================================

resource "aws_iam_policy" "glue_vpc_policy" {
  name = "${var.resource_prefix}-${var.environment}-afs1-ctr-flattened-glue-vpc-policy-cm"
  path = "/customer-managed/"

  policy = data.aws_iam_policy_document.glue_vpc_access.json
}

resource "aws_iam_role_policy_attachment" "glue_redshift_vpc_attach" {
  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = aws_iam_policy.glue_vpc_policy.arn
}


# ============================================================
# AWS MANAGED POLICIES
# ============================================================

locals {
  glue_managed_policies = {
    glue_service_role = "arn:aws:iam::aws:policy/service-role/AWSGlueServiceRole"
  }
}

resource "aws_iam_role_policy_attachment" "glue_redshift_managed_policy_attach" {
  for_each = local.glue_managed_policies

  role       = aws_iam_role.glue_redshift_role.name
  policy_arn = each.value
}
