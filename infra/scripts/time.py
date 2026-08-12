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
