terraform import -var-file="contact-evaluations-pipeline.tfvars" "module.compute.aws_glue_security_configuration.ce_sec_config" "npsenvoicedev-dev-afs1-ce-sec-config"

terraform import -var-file="contact-evaluations-pipeline.tfvars" "module.compute.aws_glue_crawler.ce_crawler" "npsenvoicedev-dev-afs1-ce-crawler"

      - name: Import existing Glue security configuration
        run: |
          terraform import \
            -input=false \
            -var-file="${{ env.TFVARS_FILE }}" \
            "module.compute.aws_glue_security_configuration.ce_sec_config" \
            "npsenvoicedev-dev-afs1-ce-sec-config" || true

      - name: Import existing Glue crawler
        run: |
          terraform import \
            -input=false \
            -var-file="${{ env.TFVARS_FILE }}" \
            "module.compute.aws_glue_crawler.ce_crawler" \
            "npsenvoicedev-dev-afs1-ce-crawler" || true

git rm --cached infra/env/dev/*.tfvars
git rm --cached infra/env/sit/*.tfvars
git rm --cached infra/env/uat/*.tfvars

git add .gitignore
git commit -m "Stop tracking environment tfvars files"
git push
