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
