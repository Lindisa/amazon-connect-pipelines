# ============================================================
# GLOBAL
# ============================================================

environment     = "dev"
resource_prefix = "npsenvoicedev"


# ============================================================
# S3
# ============================================================

source_bucket_name = "afs1-npsenvoicedev-voice-connect-storage-dev"

target_bucket_name = "afs1-npsenvoicedev-voice-connect-storage-dev"

glue_scripts_bucket_name = "afs1-dev-conivr-voice-connect-storage-etl-script-store"


# ============================================================
# EXISTING ENTERPRISE REDSHIFT/JDBC GLUE CONNECTION
# ============================================================

glue_connection_name = "npsenvoicedev_Amazon_Redshift_Connection"


# ============================================================
# EXISTING SHARED NETWORK GLUE CONNECTION
# ============================================================

network_glue_connection_name = "npsenvoicedev-dev-afs1_Network_Connection_dev"

glue_connection_availability_zone = "af-south-1b"

glue_connection_subnet_id = "subnet-0ae15468a931bd990"

glue_connection_security_group_ids = [
  "sg-021affc8ab737a277"
]


# ============================================================
# EXISTING GLUE CATALOG
# ============================================================

glue_catalog_database = "connect_db_dev"

glue_catalog_table = "ctr"


# ============================================================
# REDSHIFT
# ============================================================

redshift_target_table = "public.ctr_flattened"

redshift_staging_table = "public.ctr_flattened_staging"

redshift_role_arn = "arn:aws:iam::922783576687:role/customer-managed/svc-s3-access-npsenvoicedev-dev-voice-redshift-cm"


# ============================================================
# KMS
# ============================================================

kms_key_arn = "arn:aws:kms:af-south-1:922783576687:key/mrk-7085ea86aeea4c39ba7c7184f2232ed1"

s3_script_store_kms_key_arn = "arn:aws:kms:af-south-1:922783576687:key/mrk-7085ea86aeea4c39ba7c7184f2232ed1"
