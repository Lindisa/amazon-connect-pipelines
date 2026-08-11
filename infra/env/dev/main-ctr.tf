# ============================================================
# SECURITY MODULE
# ============================================================

module "security" {
  source = "../../modules/security"

  environment     = var.environment
  resource_prefix = var.resource_prefix

  ############################################################
  # S3
  ############################################################

  source_bucket_name = var.source_bucket_name
  scripts_bucket     = var.glue_scripts_bucket_name
  temp_bucket        = var.target_bucket_name

  ############################################################
  # KMS
  ############################################################

  kms_key_arn                 = var.kms_key_arn
  s3_script_store_kms_key_arn = var.s3_script_store_kms_key_arn
}


# ============================================================
# COMPUTE MODULE
# ============================================================

module "compute" {
  source = "../../modules/compute"

  environment     = var.environment
  resource_prefix = var.resource_prefix

  ############################################################
  # GLUE IAM ROLE
  ############################################################

  glue_redshift_role_arn = module.security.glue_redshift_role_arn

  ############################################################
  # S3
  ############################################################

  scripts_bucket = var.glue_scripts_bucket_name
  temp_bucket    = var.target_bucket_name

  ############################################################
  # EXISTING ENTERPRISE REDSHIFT/JDBC GLUE CONNECTION
  ############################################################

  glue_connection_name = var.glue_connection_name

  ############################################################
  # EXISTING SHARED NETWORK GLUE CONNECTION
  ############################################################

  network_glue_connection_name = var.network_glue_connection_name

  glue_connection_availability_zone = var.glue_connection_availability_zone
  glue_connection_subnet_id         = var.glue_connection_subnet_id
  glue_connection_security_group_ids = var.glue_connection_security_group_ids

  ############################################################
  # EXISTING GLUE CATALOG
  ############################################################

  glue_catalog_database = var.glue_catalog_database
  glue_catalog_table    = var.glue_catalog_table

  ############################################################
  # REDSHIFT
  ############################################################

  redshift_target_table  = var.redshift_target_table
  redshift_staging_table = var.redshift_staging_table
  redshift_role_arn      = var.redshift_role_arn

  ############################################################
  # KMS
  ############################################################

  kms_key_arn = var.kms_key_arn
}
