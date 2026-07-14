data "aws_s3_bucket" "source" {
  bucket = var.source_bucket_name
}

data "aws_s3_bucket" "target" {
  bucket = var.target_bucket_name
}