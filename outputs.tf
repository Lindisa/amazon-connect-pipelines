terraform import \
  -var-file="contact-evaluations-pipeline.tfvars" \
  'module.compute.aws_glue_security_configuration.ce_sec_config' \
  'npsenvoicedev-dev-afs1-ce-sec-config'


terraform import \
  -var-file="contact-evaluations-pipeline.tfvars" \
  'module.compute.aws_glue_crawler.ce_crawler' \
  'npsenvoicedev-dev-afs1-ce-crawler'

terraform plan \
  -var-file="contact-evaluations-pipeline.tfvars"
