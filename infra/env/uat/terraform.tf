terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.77.0"
    }
  }

  required_version = "~> 1.8"

  backend "s3" {
    bucket = "npsenvoicesit-uat-afs1-terraform-state"
    key    = "terraform/uat/af-south-1/contact-evaluations-pipeline/terraform.tfstate"
    region = "af-south-1"

    dynamodb_table = "npsenvoicesit-uat-afs1-terraform-state-lock"

    encrypt   = true
    kms_key_id = "arn:aws:kms:af-south-1:782747290936:key/e6c1bff8-db84-4442-b66f-309c7082054d"

    assume_role = {
      role_arn     = "arn:aws:iam::196004716891:role/customer-managed/npsenvoicedev-uat-terraform-role-cm"
      session_name = "terraform"
    }
  }
}