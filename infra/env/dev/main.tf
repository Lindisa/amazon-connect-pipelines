module "storage" {
  source = "../../modules/storage"

  resource_prefix = var.resource_prefix

  environment        = var.environment
  source_bucket_name = var.source_bucket_name
  target_bucket_name = var.target_bucket_name

  glue_crawler_role_arn = module.security.glue_preprocess_role_arn
}

module "security" {
  source = "../../modules/security"

  resource_prefix = var.resource_prefix

  environment             = var.environment
  source_bucket_name      = var.source_bucket_name
  target_bucket_name      = var.target_bucket_name
  glue_scripts_bucket_name = var.glue_scripts_bucket_name
  s3_script_store_kms_key_arn = var.s3_script_store_kms_key_arn

  kms_key_arn = var.kms_key_arn
}

module "compute" {
  source = "../../modules/compute"

  environment     = var.environment
  resource_prefix = var.resource_prefix

  glue_preprocess_role_arn = module.security.glue_preprocess_role_arn
  glue_redshift_role_arn   = module.security.glue_redshift_role_arn
  glue_crawler_role_arn    = var.glue_crawler_role_arn

  redshift_role_arn = var.redshift_role_arn

  glue_connection_availability_zone        = var.glue_connection_availability_zone
  glue_connection_subnet_id                = var.glue_connection_subnet_id
  glue_connection_name_security_group_id_list = var.glue_connection_name_security_group_id_list
  s3_script_store_kms_key_arn              = var.s3_script_store_kms_key_arn

  scripts_bucket = var.glue_scripts_bucket_name
  temp_bucket    = var.target_bucket_name

  # ==========================================================
  # CONTACT EVALUATIONS PIPELINE SETTINGS
  # ==========================================================

  source_prefix = var.source_prefix

  # Existing Enterprise Glue Connection
  glue_connection_name = var.glue_connection_name

  # Glue Catalog
  glue_catalog_database = var.glue_catalog_database
  glue_catalog_table    = var.glue_catalog_table

  # Redshift
  redshift_database     = var.redshift_database
  redshift_target_table = var.redshift_target_table

  # KMS
  kms_key_arn = var.kms_key_arn
}