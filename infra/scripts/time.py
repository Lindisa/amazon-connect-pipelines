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
),
validation AS (
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
)
SELECT
    contact_id,
    expected_latest_timestamp,
    flattened_timestamp,
    flattened_row_count,
    validation_status
FROM validation
ORDER BY
    CASE
        WHEN validation_status = 'MATCH - LATEST PRESERVED' THEN 2
        ELSE 1
    END,
    contact_id;
