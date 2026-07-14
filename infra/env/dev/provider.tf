provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      TeamCode    = "voice"
      CostCenter  = "103334"
      AppId       = "BSN0016411"
      Environment = var.environment
      ProjectId   = var.project_id
      ProjectName = var.project_name
    }
  }

  assume_role {
    role_arn     = "arn:aws:iam::922783576687:role/customer-managed/npsenvoicedev-dev-terraform-role-cm"
    session_name = "terraform"
  }
}

data "aws_caller_identity" "current" {}