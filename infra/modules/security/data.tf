data "aws_iam_policy_document" "glue_assume_role" {

  statement {
    actions = ["sts:AssumeRole"]
    effect  = "Allow"

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }
  }

  statement {
    sid    = "RemediatedDenyExternalServiceAccessBlock"
    effect = "Deny"

    actions = ["sts:AssumeRole"]

    principals {
      type        = "Service"
      identifiers = ["glue.amazonaws.com"]
    }

    condition {
      test     = "StringNotEqualsIfExists"
      variable = "aws:SourceOrgID"
      values   = ["o-iw0b4oudhj"]
    }

    condition {
      test     = "Null"
      variable = "aws:SourceAccount"
      values   = ["false"]
    }

    condition {
      test     = "Bool"
      variable = "aws:PrincipalIsAWSService"
      values   = ["true"]
    }
  }
}

# S3 ACCESS

data "aws_iam_policy_document" "glue_s3_access" {

  statement {
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]

    resources = [
      "arn:aws:s3:::${var.source_bucket_name}",
      "arn:aws:s3:::${var.target_bucket_name}",
      "arn:aws:s3:::${var.glue_scripts_bucket_name}"
    ]
  }

  statement {
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:PutObject"
    ]

    resources = [
      "arn:aws:s3:::${var.source_bucket_name}/*",
      "arn:aws:s3:::${var.target_bucket_name}/*",
      "arn:aws:s3:::${var.glue_scripts_bucket_name}/*"
    ]
  }
}

# LOGS + KMS ACCESS

data "aws_iam_policy_document" "glue_logs_kms_access" {

  statement {
    effect = "Allow"

    actions = [
      "logs:CreateLogGroup",
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:AssociateKmsKey"
    ]

    resources = ["*"]
  }

  statement {
    effect = "Allow"

    actions = [
      "kms:Decrypt",
      "kms:Encrypt",
      "kms:GenerateDataKey"
    ]

    resources = [
      var.kms_key_arn,
      var.s3_script_store_kms_key_arn
    ]
  }
}

# ACCESS TO VPC

data "aws_iam_policy_document" "glue_vpc_access" {

  statement {
    effect = "Allow"

    actions = [
      "logs:PutLogEvents",
      "logs:CreateLogStream",
      "logs:CreateLogGroup",

      "ec2:UnassignPrivateIpAddresses",
      "ec2:DescribeVpcEndpoints",
      "ec2:DescribeVpcAttribute",
      "ec2:DescribeSubnets",
      "ec2:DescribeSecurityGroups",
      "ec2:DescribeRouteTables",
      "ec2:DescribeNetworkInterfaces",
      "ec2:DeleteNetworkInterface",
      "ec2:CreateTags",
      "ec2:CreateNetworkInterfacePermission",
      "ec2:CreateNetworkInterface",
      "ec2:AssignPrivateIpAddresses",

      "cloudwatch:PutMetricData"
    ]

    resources = ["*"]
  }
}