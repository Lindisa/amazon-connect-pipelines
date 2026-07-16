Great. Run these checks in Redshift.

## 1. Confirm rows loaded

```sql
SELECT COUNT(*) AS total_rows
FROM public.ctr_flattened;
```

## 2. Check the flattened columns exist

```sql
SELECT
    column_name,
    data_type,
    ordinal_position
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'ctr_flattened'
ORDER BY ordinal_position;
```

You should see separate columns such as:

```text
agent_arn
agent_username
agent_routing_profile_name
queue_name
queue_duration
customer_endpoint_address
tag_billing_cost_center
quality_agent_audio_score
```

## 3. Verify agent and queue fields are directly queryable

```sql
SELECT
    contact_id,
    agent_arn,
    agent_username,
    agent_routing_profile_name,
    queue_arn,
    queue_name,
    queue_duration,
    initiation_timestamp,
    last_update_timestamp
FROM public.ctr_flattened
WHERE agent_arn IS NOT NULL
LIMIT 20;
```

## 4. Verify tags are extracted into columns

```sql
SELECT
    contact_id,
    tag_billing_cost_center,
    tag_billing_department,
    tag_billing_division,
    tag_speech_analytics,
    tag_aws_connect_instance_id,
    tag_aws_connect_system_endpoint
FROM public.ctr_flattened
WHERE tag_billing_cost_center IS NOT NULL
   OR tag_billing_department IS NOT NULL
   OR tag_aws_connect_instance_id IS NOT NULL
LIMIT 20;
```

## 5. Verify missing fields are stored as SQL NULL

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(agent_arn) AS rows_with_agent,
    COUNT(queue_name) AS rows_with_queue,
    COUNT(tag_billing_cost_center) AS rows_with_billing_tag,
    COUNT(quality_agent_audio_score) AS rows_with_quality_score
FROM public.ctr_flattened;
```

`COUNT(column_name)` excludes nulls, so this shows which records contain each field.

## 6. Check that the old long JSON columns are not present

```sql
SELECT column_name
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'ctr_flattened'
  AND column_name IN (
      'agent',
      'queue',
      'customer_endpoint',
      'system_endpoint',
      'recording',
      'quality_metrics',
      'contact_lens'
  );
```

This should return no rows.

## 7. Verify the remaining `SUPER` columns

```sql
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'public'
  AND table_name = 'ctr_flattened'
  AND column_name IN (
      'agent_hierarchy_groups',
      'agent_state_transitions',
      'contact_lens_analytics_modes',
      'contact_lens_redaction_entities',
      'contact_lens_summary_configuration',
      'quality_agent_audio_issues',
      'quality_customer_audio_issues',
      'media_streams',
      'recordings',
      'references_data'
  )
ORDER BY column_name;
```

They should show as:

```text
super
```

## 8. Query values inside a `SUPER` array

For recordings:

```sql
SELECT
    contact_id,
    recording_item.Location::VARCHAR AS recording_location,
    recording_item.MediaStreamType::VARCHAR AS media_stream_type,
    recording_item.ParticipantType::VARCHAR AS participant_type,
    recording_item.Status::VARCHAR AS recording_status
FROM public.ctr_flattened AS ctr,
     ctr.recordings AS recording_item
WHERE ctr.recordings IS NOT NULL
LIMIT 20;
```

For media streams:

```sql
SELECT
    contact_id,
    media_item.Type::VARCHAR AS media_stream_type
FROM public.ctr_flattened AS ctr,
     ctr.media_streams AS media_item
WHERE ctr.media_streams IS NOT NULL
LIMIT 20;
```

## 9. Check duplicates and versions

Because your target is append-only:

```sql
SELECT
    contact_id,
    COUNT(*) AS version_count,
    MIN(last_update_timestamp) AS earliest_version,
    MAX(last_update_timestamp) AS latest_version
FROM public.ctr_flattened
GROUP BY contact_id
HAVING COUNT(*) > 1
ORDER BY version_count DESC
LIMIT 20;
```

## 10. View only the latest version per contact

```sql
SELECT *
FROM public.ctr_flattened
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY contact_id
    ORDER BY last_update_timestamp DESC NULLS LAST,
             etl_loaded_timestamp DESC
) = 1
LIMIT 20;
```

The most important verification query is this one:

```sql
SELECT
    contact_id,
    agent_username,
    agent_routing_profile_name,
    queue_name,
    customer_endpoint_address,
    tag_billing_cost_center,
    quality_agent_audio_score
FROM public.ctr_flattened
LIMIT 50;
```

If that returns separate values rather than one long JSON object, the flattening worked.
