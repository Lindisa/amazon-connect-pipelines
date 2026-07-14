# ============================================================
# GLOBAL
# ============================================================

aws_region     = "af-south-1"
environment    = "dev"
project_id     = "contact-evaluations-pipeline"
project_name   = "contact-evaluations-pipeline"
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
# EXISTING ENTERPRISE GLUE CONNECTION
# ============================================================

glue_connection_name = "Amazon_Redshift_Connection_dev"


# ============================================================
# NETWORK GLUE CONNECTION REQUIRED FOR SCP/VPC CONTEXT
# ============================================================

glue_connection_availability_zone = "af-south-1a"

glue_connection_subnet_id = "subnet-0f94a798d12f04a42"

glue_connection_name_security_group_id_list = [
  "sg-021affc8ab737a277"
]


# ============================================================
# EXISTING GLUE CATALOG
# ============================================================

glue_catalog_database = "connect_db_dev"

glue_catalog_table = "contactevaluations_pre_processed"

redshift_role_arn = "arn:aws:iam::922783576687:role/customer-managed/svc-s3-access-npsenvoicedev-dev-voice-redshift-cm"


# ============================================================
# REDSHIFT
# ============================================================

redshift_database = "amazonconnectdatawarehouse"

redshift_target_table = "public.contact_evaluations"


# ============================================================
# CRAWLER ROLE AND KMS
# ============================================================

glue_crawler_role_arn = "arn:aws:iam::922783576687:role/customer-managed/aws-service-s3-access-npsenvoicedev-dev-voice-cm"

s3_script_store_kms_key_arn = "arn:aws:kms:af-south-1:782747290936:key/e42a223a-4e35-462b-a438-9d59b23be93a"

kms_key_arn = "arn:aws:kms:af-south-1:922783576687:key/mrk-7085ea86aeea4c39ba7c7184f2232ed1"