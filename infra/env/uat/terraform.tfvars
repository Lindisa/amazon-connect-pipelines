```hcl
# ==========================================================
# GLOBAL
# ==========================================================

aws_region      = "af-south-1"
environment     = "uat"

project_id      = "contact-evaluations-pipeline"
project_name    = "contact-evaluations-pipeline"
resource_prefix = "npsenvoiceuat"


# ==========================================================
# S3
# ==========================================================

source_bucket_name = "afs1-npsenvoicesit-voice-connect-storage-uat"

source_prefix = "connect/absa-contact-centre-uat/ContactEvaluations/"

target_bucket_name = "afs1-npsenvoicesit-voice-connect-storage-uat"

target_prefix = "connect/absa-contact-centre-uat/ContactEvaluations/pre-processed/"

glue_scripts_bucket_name = "afs1-uat-conivr-voice-connect-storage-etl-script-store"


# ==========================================================
# EXISTING ENTERPRISE REDSHIFT/JDBC GLUE CONNECTION
# ==========================================================

glue_connection_name = "Amazon_Redshift_Connection_uat"


# ==========================================================
# EXISTING SHARED NETWORK GLUE CONNECTION
# ==========================================================

network_glue_connection_name = "REPLACE_WITH_EXISTING_UAT_NETWORK_CONNECTION_NAME"


# ==========================================================
# EXISTING GLUE CATALOG
# ==========================================================

glue_catalog_database = "connect_db_uat"
glue_catalog_table    = "contactevaluations_pre_processed"


# ==========================================================
# REDSHIFT
# ==========================================================

redshift_database     = "amazonconnectdatawarehouse"
redshift_target_table = "public.contact_evaluations"

redshift_role_arn = "arn:aws:iam::196004716891:role/customer-managed/svc-s3-access-npsenvoicedev-uat-voice-redshift-cm"


# ==========================================================
# IAM AND KMS
# ==========================================================

glue_crawler_role_arn = "arn:aws:iam::196004716891:role/customer-managed/aws-service-s3-access-npsenvoicedev-uat-voice-cm"

s3_script_store_kms_key_arn = "arn:aws:kms:af-south-1:782747290936:key/e6c1bff8-db84-4442-b66f-309c7082054d"

kms_key_arn = "arn:aws:kms:af-south-1:196004716891:key/mrk-818aa7ac95234365ab7633e9a1567068"
```
