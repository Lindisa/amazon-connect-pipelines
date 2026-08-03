# ============================================================
# SECURITY MODULE
# ============================================================

module "security" {
  source = "../../modules/security"

  resource_prefix = var.resource_prefix
  environment     = var.environment

  source_bucket_name       = var.source_bucket_name
  target_bucket_name       = var.target_bucket_name
  glue_scripts_bucket_name = var.glue_scripts_bucket_name

  kms_key_arn                 = var.kms_key_arn
  s3_script_store_kms_key_arn = var.s3_script_store_kms_key_arn
}


# ============================================================
# STORAGE MODULE
# ============================================================

module "storage" {
  source = "../../modules/storage"

  resource_prefix = var.resource_prefix
  environment     = var.environment

  source_bucket_name = var.source_bucket_name
  target_bucket_name = var.target_bucket_name

  glue_crawler_role_arn = var.glue_crawler_role_arn
}


# ============================================================
# COMPUTE MODULE
# ============================================================

module "compute" {
  source = "../../modules/compute"

  resource_prefix = var.resource_prefix
  environment     = var.environment

  # ==========================================================
  # IAM ROLES
  # ==========================================================

  glue_preprocess_role_arn = module.security.glue_preprocess_role_arn
  glue_redshift_role_arn   = module.security.glue_redshift_role_arn
  glue_crawler_role_arn    = var.glue_crawler_role_arn

  redshift_role_arn = var.redshift_role_arn

  # ==========================================================
  # GLUE CONNECTIONS
  # ==========================================================

  network_glue_connection_name  = var.network_glue_connection_name
  redshift_glue_connection_name = var.redshift_glue_connection_name

  glue_connection_availability_zone = var.glue_connection_availability_zone
  glue_connection_subnet_id          = var.glue_connection_subnet_id
  glue_connection_security_group_ids = var.glue_connection_security_group_ids

  # ==========================================================
  # REDSHIFT JDBC
  # ==========================================================

  redshift_jdbc_url = var.redshift_jdbc_url
  redshift_username = var.redshift_username
  redshift_password = var.redshift_password

  # ==========================================================
  # S3
  # ==========================================================

  scripts_bucket = var.glue_scripts_bucket_name
  temp_bucket    = var.target_bucket_name

  source_prefix = var.source_prefix
  target_prefix = var.target_prefix

  # ==========================================================
  # GLUE CATALOG
  # ==========================================================

  glue_catalog_database = var.glue_catalog_database
  glue_catalog_table    = var.glue_catalog_table

  # ==========================================================
  # REDSHIFT TARGET
  # ==========================================================

  redshift_target_table = var.redshift_target_table

  # ==========================================================
  # KMS
  # ==========================================================

  kms_key_arn = var.kms_key_arn

  depends_on = [
    module.security,
    module.storage
  ]
}
