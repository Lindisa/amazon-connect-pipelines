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
    c.contact_id,
    c.expected_latest_timestamp,
    f.flattened_timestamp,
    f.flattened_row_count,
    CASE
        WHEN f.flattened_row_count > 1
            THEN 'DUPLICATE'
        WHEN f.flattened_timestamp = c.expected_latest_timestamp
            THEN 'MATCH - LATEST PRESERVED'
        ELSE 'TIMESTAMP MISMATCH'
    END AS validation_status
FROM ctr_latest c
INNER JOIN flattened f
    ON c.contact_id = f.contact_id
ORDER BY
    CASE
        WHEN f.flattened_row_count = 1
         AND f.flattened_timestamp = c.expected_latest_timestamp
            THEN 2
        ELSE 1
    END,
    c.contact_id;
