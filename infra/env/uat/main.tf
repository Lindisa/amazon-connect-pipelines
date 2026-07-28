module "compute" {
  source = "../../modules/compute"

  environment     = var.environment
  resource_prefix = var.resource_prefix

  glue_preprocess_role_arn = module.security.glue_preprocess_role_arn
  glue_redshift_role_arn   = module.security.glue_redshift_role_arn
  redshift_role_arn        = var.redshift_role_arn
  glue_crawler_role_arn    = var.glue_crawler_role_arn

  s3_script_store_kms_key_arn = var.s3_script_store_kms_key_arn

  scripts_bucket = var.glue_scripts_bucket_name
  temp_bucket    = var.target_bucket_name

  source_prefix = var.source_prefix

  ############################################################
  # EXISTING ENTERPRISE REDSHIFT/JDBC GLUE CONNECTION
  ############################################################

  glue_connection_name = var.glue_connection_name

  ############################################################
  # EXISTING SHARED NETWORK GLUE CONNECTION
  ############################################################

  network_glue_connection_name = var.network_glue_connection_name

  ############################################################
  # EXISTING GLUE CATALOG
  ############################################################

  glue_catalog_database = var.glue_catalog_database
  glue_catalog_table    = var.glue_catalog_table

  ############################################################
  # REDSHIFT
  ############################################################

  redshift_database     = var.redshift_database
  redshift_target_table = var.redshift_target_table

  ############################################################
  # KMS
  ############################################################

  kms_key_arn = var.kms_key_arn
}
