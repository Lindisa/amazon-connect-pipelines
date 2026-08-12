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
Use this query to compare each `contact_id` in `public.ctr_flattened` against the latest version in `public.ctr`:

```sql
WITH ctr_latest AS (
    SELECT
        contact_id,
        MAX(last_update_timestamp) AS expected_latest_timestamp
    FROM public.ctr
    WHERE contact_id IS NOT NULL
    GROUP BY contact_id
),
flattened AS (
    SELECT
        contact_id,
        COUNT(*) AS flattened_row_count,
        MAX(last_update_timestamp) AS flattened_timestamp
    FROM public.ctr_flattened
    WHERE contact_id IS NOT NULL
    GROUP BY contact_id
)
SELECT
    COALESCE(c.contact_id, f.contact_id) AS contact_id,
    c.expected_latest_timestamp,
    f.flattened_timestamp,
    f.flattened_row_count,
    CASE
        WHEN c.contact_id IS NULL
            THEN 'NOT IN CTR'
        WHEN f.contact_id IS NULL
            THEN 'MISSING FROM FLATTENED'
        WHEN f.flattened_row_count > 1
            THEN 'DUPLICATE IN FLATTENED'
        WHEN f.flattened_timestamp = c.expected_latest_timestamp
            THEN 'MATCH - LATEST PRESERVED'
        ELSE 'TIMESTAMP MISMATCH'
    END AS validation_status
FROM ctr_latest c
FULL OUTER JOIN flattened f
    ON c.contact_id = f.contact_id
ORDER BY
    CASE
        WHEN validation_status = 'MATCH - LATEST PRESERVED' THEN 2
        ELSE 1
    END,
    contact_id;
```

For a quick summary:

```sql
WITH ctr_latest AS (
    SELECT
        contact_id,
        MAX(last_update_timestamp) AS expected_timestamp
    FROM public.ctr
    WHERE contact_id IS NOT NULL
    GROUP BY contact_id
),
flattened AS (
    SELECT
        contact_id,
        COUNT(*) AS row_count,
        MAX(last_update_timestamp) AS actual_timestamp
    FROM public.ctr_flattened
    WHERE contact_id IS NOT NULL
    GROUP BY contact_id
),
validation AS (
    SELECT
        c.contact_id,
        CASE
            WHEN f.contact_id IS NULL
                THEN 'MISSING'
            WHEN f.row_count > 1
                THEN 'DUPLICATE'
            WHEN f.actual_timestamp = c.expected_timestamp
                THEN 'LATEST MATCHES'
            ELSE 'TIMESTAMP MISMATCH'
        END AS result
    FROM ctr_latest c
    LEFT JOIN flattened f
        ON c.contact_id = f.contact_id
)
SELECT
    result,
    COUNT(*) AS contact_count
FROM validation
GROUP BY result
ORDER BY result;
```

The successful outcome is:

* `LATEST MATCHES` for processed contacts
* Zero `DUPLICATE`
* Zero `TIMESTAMP MISMATCH`

`MISSING` may be expected if `public.ctr_flattened` has only processed recent incremental partitions while `public.ctr` contains complete historical data.
