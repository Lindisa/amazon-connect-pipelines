# ============================================================
# CTR FLATTENED REDSHIFT ETL SCRIPT
# ============================================================

resource "aws_s3_object" "ctr_flattened_redshift_script" {
  bucket = var.scripts_bucket

  key = "ctr-flattened/load-ctr-flattened-to-redshift.py"

  source = "${path.module}/../../../scripts/load-flattened-ctr-to-redshift.py"

  etag = filemd5("${path.module}/../../../scripts/load-flattened-ctr-to-redshift.py")
}
