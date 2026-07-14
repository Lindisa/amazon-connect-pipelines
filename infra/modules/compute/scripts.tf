resource "aws_s3_object" "preprocess_script" {
  bucket = var.scripts_bucket

  key = "contact-evaluations/contact-evaluations-pre-process.py"

  source = "../../../scripts/contact-evaluations-pre-process.py"

  etag = filemd5("../../../scripts/contact-evaluations-pre-process.py")
}

resource "aws_s3_object" "redshift_script" {
  bucket = var.scripts_bucket

  key = "contact-evaluations/load-contact-evaluations-to-redshift.py"

  source = "../../../scripts/load-contact-evaluations-to-redshift.py"

  etag = filemd5("../../../scripts/load-contact-evaluations-to-redshift.py")
}