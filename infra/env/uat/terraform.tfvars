# ==========================================================
# GLOBAL
# ==========================================================

aws_region      = "af-south-1"
environment     = "uat"

project_id      = "contact-lens-pipeline"
project_name    = "contact-lens-pipeline"
resource_prefix = "npsenvoiceuat"

# ==========================================================
# S3
# ==========================================================

source_bucket_name = "afs1-npsenvoicesit-voice-connect-storage-uat"

# Updated to support partitioned ingestion
source_prefix = "connect/absa-contact-centre-dev/ContactEvaluations/"

target_bucket_name = "afs1-npsenvoicesit-voice-connect-storage-uat"

# Partitioned parquet output
target_prefix = "connect/absa-contact-centre-sit/ContactEvaluations/pre-processed/"

glue_scripts_bucket_name = "afs1-uat-conivr-voice-connect-storage-etl-script-store"

# ==========================================================
# EXISTING ENTERPRISE GLUE CONNECTION
# ==========================================================

glue_connection_name = "Amazon_Redshift_Connection_uat"

# NETWORK GLUE CONNECTION REQUIRED FOR SCP/VPC CONTEXT

glue_connection_availability_zone       = "af-south-1a"
glue_connection_subnet_id               = "subnet-0980c565baf160215"
glue_connection_name_security_group_id_list = ["sg-035856ce270efdf8d"]

# ==========================================================
# EXISTING GLUE CATALOG
# ==========================================================

glue_catalog_database = "connect_db_uat"

glue_catalog_table = "contactevaluations_pre_processed"

# ==========================================================
# REDSHIFT
# ==========================================================

redshift_database = "amazonconnectdatawarehouse"

redshift_target_table = "public.contact_evaluations"

redshift_role_arn = "arn:aws:iam::196004716891:role/customer-managed/svc-s3-access-npsenvoicedev-uat-voice-redshift-cm"

# ==========================================================
# KMS
# ==========================================================

glue_crawler_role_arn = "arn:aws:iam::196004716891:role/customer-managed/aws-service-s3-access-npsenvoicedev-uat-voice-cm"

s3_script_store_kms_key_arn = "arn:aws:kms:af-south-1:782747290936:key/e6c1bff8-db84-4442-b66f-309c7082054d"

kms_key_arn = "arn:aws:kms:af-south-1:196004716891:key/mrk-818aa7ac95234365ab7633e9a1567068"