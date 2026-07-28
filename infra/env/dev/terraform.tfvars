```hcl
# ============================================================
# GLOBAL
# ============================================================

aws_region      = "af-south-1"
environment     = "dev"

project_id      = "contact-evaluations-pipeline"
project_name    = "contact-evaluations-pipeline"
resource_prefix = "npsenvoicedev"


# ============================================================
# S3
# ============================================================

source_bucket_name = "afs1-npsenvoicedev-voice-connect-storage-dev"

source_prefix = "connect/absa-contact-centre-dev/ContactEvaluations/"

target_bucket_name = "afs1-npsenvoicedev-voice-connect-storage-dev"

target_prefix = "connect/absa-contact-centre-dev/ContactEvaluations/processed/"

glue_scripts_bucket_name = "afs1-dev-conivr-voice-connect-storage-etl-script-store"


# ============================================================
# EXISTING ENTERPRISE REDSHIFT/JDBC GLUE CONNECTION
# ============================================================

glue_connection_name = "Amazon_Redshift_Connection_dev"


# ============================================================
# EXISTING SHARED NETWORK GLUE CONNECTION
# ============================================================

network_glue_connection_name = "REPLACE_WITH_EXISTING_DEV_NETWORK_CONNECTION_NAME"


# ============================================================
# EXISTING GLUE CATALOG
# ============================================================

glue_catalog_database = "connect_db_dev"

glue_catalog_table = "contactevaluations_pre_processed"


# ============================================================
# REDSHIFT
# ============================================================

redshift_database = "amazonconnectdatawarehouse"

redshift_target_table = "public.contact_evaluations"

redshift_role_arn = "arn:aws:iam::922783576687:role/customer-managed/svc-s3-access-npsenvoicedev-dev-voice-redshift-cm"


# ============================================================
# CRAWLER ROLE AND KMS
# ============================================================

glue_crawler_role_arn = "arn:aws:iam::922783576687:role/customer-managed/aws-service-s3-access-npsenvoicedev-dev-voice-cm"

s3_script_store_kms_key_arn = "arn:aws:kms:af-south-1:782747290936:key/e42a223a-4e35-462b-a438-9d59b23be93a"

kms_key_arn = "arn:aws:kms:af-south-1:922783576687:key/mrk-7085ea86aeea4c39ba7c7184f2232ed1"
```
