data "aws_caller_identity" "current" {}

resource "aws_iam_role" "glue_preprocess_role" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-evaluations-glue-preprocess-role-cm"
  path = "/customer-managed/"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json

  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/service-boundary/customer/absa-default-cldfrc-service-boundary-customer"

  tags = {
    dlp-exp = "true"
  }
}

resource "aws_iam_role" "glue_redshift_role" {
  name = "${var.resource_prefix}-${var.environment}-afs1-contact-evaluations-glue-redshift-role-cm"
  assume_role_policy = data.aws_iam_policy_document.glue_assume_role.json
  path = "/customer-managed/"

  permissions_boundary = "arn:aws:iam::${data.aws_caller_identity.current.account_id}:policy/service-boundary/customer/absa-default-cldfrc-service-boundary-customer"

  tags = {
    dlp-exp = "true"
  }
}