SELECT
    (
        SELECT COUNT(DISTINCT contact_id)
        FROM public.ctr
        WHERE contact_id IS NOT NULL
    ) AS ctr_contacts,

    (
        SELECT COUNT(DISTINCT contact_id)
        FROM public.ctr_flattened
        WHERE contact_id IS NOT NULL
    ) AS flattened_contacts,

    (
        SELECT COUNT(DISTINCT c.contact_id)
        FROM public.ctr c
        INNER JOIN public.ctr_flattened f
            ON c.contact_id = f.contact_id
        WHERE c.contact_id IS NOT NULL
    ) AS matching_contacts;


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
            WHEN f.row_count > 1
                THEN 'DUPLICATE'
            WHEN f.actual_timestamp = c.expected_timestamp
                THEN 'LATEST MATCHES'
            ELSE 'TIMESTAMP MISMATCH'
        END AS validation_status
    FROM ctr_latest c
    INNER JOIN flattened f
        ON c.contact_id = f.contact_id
)
SELECT
    validation_status,
    COUNT(*) AS contact_count
FROM validation
GROUP BY validation_status
ORDER BY validation_status;
