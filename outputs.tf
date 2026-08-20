Run these in DEV, SIT and UAT after the initial load. I’ve included the expected result for each.

### 1. Confirm target and staging schemas match

```sql
WITH target_columns AS (
    SELECT ordinal_position, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'ctr_flattened'
),
staging_columns AS (
    SELECT ordinal_position, column_name, data_type
    FROM information_schema.columns
    WHERE table_schema = 'public'
      AND table_name = 'ctr_flattened_staging'
)
SELECT
    COALESCE(t.ordinal_position, s.ordinal_position) AS position,
    t.column_name AS target_column,
    s.column_name AS staging_column,
    t.data_type AS target_type,
    s.data_type AS staging_type
FROM target_columns t
FULL OUTER JOIN staging_columns s
    ON t.ordinal_position = s.ordinal_position
WHERE COALESCE(t.column_name, '') <> COALESCE(s.column_name, '')
   OR COALESCE(t.data_type, '') <> COALESCE(s.data_type, '')
ORDER BY position;
```

Expected: **0 rows**.

### 2. Confirm load coverage

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT contact_id) AS distinct_contacts,
    MIN(initiation_timestamp) AS earliest_contact,
    MAX(initiation_timestamp) AS latest_contact,
    MAX(last_update_timestamp) AS latest_update,
    MAX(etl_loaded_timestamp) AS latest_etl_load
FROM public.ctr_flattened;
```

`total_rows` should equal `distinct_contacts`.

### 3. Check duplicates

```sql
SELECT
    contact_id,
    COUNT(*) AS record_count
FROM public.ctr_flattened
GROUP BY contact_id
HAVING COUNT(*) > 1
ORDER BY record_count DESC;
```

Expected: **0 rows**.

### 4. Compare the flattened table with the latest CTR records

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
        last_update_timestamp AS actual_timestamp
    FROM public.ctr_flattened
)
SELECT
    CASE
        WHEN f.contact_id IS NULL THEN 'MISSING_FROM_FLATTENED'
        WHEN c.contact_id IS NULL THEN 'EXTRA_IN_FLATTENED'
        WHEN c.expected_timestamp <> f.actual_timestamp
            THEN 'TIMESTAMP_MISMATCH'
        ELSE 'LATEST_MATCHES'
    END AS validation_status,
    COUNT(*) AS contact_count
FROM ctr_latest c
FULL OUTER JOIN flattened f
    ON c.contact_id = f.contact_id
GROUP BY 1
ORDER BY 1;
```

Expected: only `LATEST_MATCHES`.

### 5. Validate hierarchy population

```sql
SELECT
    COUNT(*) AS total_rows,
    COUNT(agent_hierarchy_groups) AS hierarchy_json_count,
    COUNT(agent_hierarchy_level_1_group_name) AS level_1_count,
    COUNT(agent_hierarchy_level_2_group_name) AS level_2_count,
    COUNT(agent_hierarchy_level_3_group_name) AS level_3_count,
    COUNT(agent_hierarchy_level_4_group_name) AS level_4_count,
    COUNT(agent_hierarchy_level_5_group_name) AS level_5_count
FROM public.ctr_flattened;
```

Hierarchy counts can differ because not every contact has every level.

### 6. Compare hierarchy columns with the original JSON

```sql
WITH hierarchy AS (
    SELECT
        contact_id,
        agent_hierarchy_groups,
        JSON_SERIALIZE(agent_hierarchy_groups) AS hierarchy_json,
        agent_hierarchy_level_1_group_name AS actual_level_1,
        agent_hierarchy_level_2_group_name AS actual_level_2,
        agent_hierarchy_level_3_group_name AS actual_level_3,
        agent_hierarchy_level_4_group_name AS actual_level_4,
        agent_hierarchy_level_5_group_name AS actual_level_5
    FROM public.ctr_flattened
    WHERE agent_hierarchy_groups IS NOT NULL
),
expected AS (
    SELECT
        *,
        NULLIF(TRIM(COALESCE(
            JSON_EXTRACT_PATH_TEXT(
                hierarchy_json, 'Level1', 'GroupName', TRUE
            ),
            JSON_EXTRACT_PATH_TEXT(
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(hierarchy_json, 0, TRUE),
                'Level1', 'GroupName', TRUE
            )
        )), '') AS expected_level_1,

        NULLIF(TRIM(COALESCE(
            JSON_EXTRACT_PATH_TEXT(
                hierarchy_json, 'Level2', 'GroupName', TRUE
            ),
            JSON_EXTRACT_PATH_TEXT(
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(hierarchy_json, 0, TRUE),
                'Level2', 'GroupName', TRUE
            )
        )), '') AS expected_level_2,

        NULLIF(TRIM(COALESCE(
            JSON_EXTRACT_PATH_TEXT(
                hierarchy_json, 'Level3', 'GroupName', TRUE
            ),
            JSON_EXTRACT_PATH_TEXT(
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(hierarchy_json, 0, TRUE),
                'Level3', 'GroupName', TRUE
            )
        )), '') AS expected_level_3,

        NULLIF(TRIM(COALESCE(
            JSON_EXTRACT_PATH_TEXT(
                hierarchy_json, 'Level4', 'GroupName', TRUE
            ),
            JSON_EXTRACT_PATH_TEXT(
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(hierarchy_json, 0, TRUE),
                'Level4', 'GroupName', TRUE
            )
        )), '') AS expected_level_4,

        NULLIF(TRIM(COALESCE(
            JSON_EXTRACT_PATH_TEXT(
                hierarchy_json, 'Level5', 'GroupName', TRUE
            ),
            JSON_EXTRACT_PATH_TEXT(
                JSON_EXTRACT_ARRAY_ELEMENT_TEXT(hierarchy_json, 0, TRUE),
                'Level5', 'GroupName', TRUE
            )
        )), '') AS expected_level_5
    FROM hierarchy
)
SELECT
    contact_id,
    expected_level_1,
    actual_level_1,
    expected_level_2,
    actual_level_2,
    expected_level_3,
    actual_level_3,
    expected_level_4,
    actual_level_4,
    expected_level_5,
    actual_level_5
FROM expected
WHERE COALESCE(expected_level_1, '<NULL>')
        <> COALESCE(actual_level_1, '<NULL>')
   OR COALESCE(expected_level_2, '<NULL>')
        <> COALESCE(actual_level_2, '<NULL>')
   OR COALESCE(expected_level_3, '<NULL>')
        <> COALESCE(actual_level_3, '<NULL>')
   OR COALESCE(expected_level_4, '<NULL>')
        <> COALESCE(actual_level_4, '<NULL>')
   OR COALESCE(expected_level_5, '<NULL>')
        <> COALESCE(actual_level_5, '<NULL>');
```

Expected: **0 rows**.

### 7. Check the five new fields for empty strings

```sql
SELECT
    SUM(CASE WHEN agent_hierarchy_level_1_group_name IS NOT NULL
              AND TRIM(agent_hierarchy_level_1_group_name) = ''
             THEN 1 ELSE 0 END) AS level_1_empty,
    SUM(CASE WHEN agent_hierarchy_level_2_group_name IS NOT NULL
              AND TRIM(agent_hierarchy_level_2_group_name) = ''
             THEN 1 ELSE 0 END) AS level_2_empty,
    SUM(CASE WHEN agent_hierarchy_level_3_group_name IS NOT NULL
              AND TRIM(agent_hierarchy_level_3_group_name) = ''
             THEN 1 ELSE 0 END) AS level_3_empty,
    SUM(CASE WHEN agent_hierarchy_level_4_group_name IS NOT NULL
              AND TRIM(agent_hierarchy_level_4_group_name) = ''
             THEN 1 ELSE 0 END) AS level_4_empty,
    SUM(CASE WHEN agent_hierarchy_level_5_group_name IS NOT NULL
              AND TRIM(agent_hierarchy_level_5_group_name) = ''
             THEN 1 ELSE 0 END) AS level_5_empty
FROM public.ctr_flattened;
```

Expected: all five values are `0`.

After the next scheduled incremental run, rerun queries **2, 3, 4 and 7** to confirm new records are loading without duplicates, timestamp mismatches or empty strings.
