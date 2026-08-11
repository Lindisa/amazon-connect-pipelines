default_arguments = {
  "--enable-metrics"               = "true"
  "--enable-spark-ui"              = "true"
  "--enable-auto-scaling"          = "true"
  "--enable-job-insights"          = "true"
  "--enable-observability-metrics" = "true"
  "--enable-glue-datacatalog"      = "true"
  "--job-bookmark-option"          = "job-bookmark-enable"
  "--job-language"                 = "python"

  # Arguments required by the updated Python script
  "--source_database"          = var.glue_catalog_database
  "--source_table"             = var.glue_catalog_table
  "--redshift_connection_name" = data.aws_glue_connection.redshift_connection.name
  "--redshift_tmp_dir"         = "s3://${var.temp_bucket}/connect/${var.environment}/ctr-flattened/redshift-temp/"
  "--redshift_schema"          = "public"
  "--target_table"             = var.redshift_target_table
  "--initial_load"             = "false"

  # Incremental processing and deduplication
  "--latest_only"              = "true"
  "--partition_lookback_hours" = "24"
  "--staging_only"             = "false"
}
