terraform {
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.77.0"
    }
  }

  required_version = "~> 1.8"

  backend "s3" {
    bucket         = "npsenvoicedev-dev-afs1-terraform-state"
    key            = "terraform/dev/af-south-1/contact-evaluations-pipeline/terraform.tfstate"
    region         = "af-south-1"
    dynamodb_table = "npsenvoicedev-dev-afs1-terraform-state-lock"
    encrypt        = true
    kms_key_id     = "arn:aws:kms:af-south-1:782747290936:key/e42a223a-4e35-462b-a438-9d59b23be93a"
    acl            = "private"

    assume_role = {
      role_arn     = "arn:aws:iam::922783576687:role/customer-managed/npsenvoicedev-dev-terraform-role-cm"
      session_name = "terraform"
    }
  }
}