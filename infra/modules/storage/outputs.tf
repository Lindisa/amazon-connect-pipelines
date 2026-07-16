You’re right. I ignored the environment detail you had already corrected me on. Use the **STL/SVL views only**.

Run this to find truncates, deletes, drops, and recreates involving `public.ctr`:

```sql
SELECT
    q.starttime,
    q.endtime,
    q.userid,
    u.usename,
    q.query,
    q.aborted,
    LISTAGG(TRIM(t.text), '')
        WITHIN GROUP (ORDER BY t.sequence) AS full_sql
FROM stl_query q
JOIN stl_querytext t
    ON q.query = t.query
LEFT JOIN pg_user u
    ON q.userid = u.usesysid
WHERE q.starttime >= DATEADD(day, -7, GETDATE())
GROUP BY
    q.starttime,
    q.endtime,
    q.userid,
    u.usename,
    q.query,
    q.aborted
HAVING
       LOWER(LISTAGG(TRIM(t.text), '')
           WITHIN GROUP (ORDER BY t.sequence)) LIKE '%truncate%public.ctr%'
    OR LOWER(LISTAGG(TRIM(t.text), '')
           WITHIN GROUP (ORDER BY t.sequence)) LIKE '%delete%from%public.ctr%'
    OR LOWER(LISTAGG(TRIM(t.text), '')
           WITHIN GROUP (ORDER BY t.sequence)) LIKE '%drop%table%public.ctr%'
    OR LOWER(LISTAGG(TRIM(t.text), '')
           WITHIN GROUP (ORDER BY t.sequence)) LIKE '%create%table%public.ctr%'
ORDER BY q.starttime DESC;
```

Also run this broader DDL check:

```sql
SELECT
    starttime,
    xid,
    sequence,
    TRIM(text) AS ddl_text
FROM stl_ddltext
WHERE starttime >= DATEADD(day, -7, GETDATE())
  AND LOWER(text) LIKE '%ctr%'
ORDER BY starttime DESC, xid, sequence;
```

And this for deletes:

```sql
SELECT
    q.starttime,
    q.userid,
    u.usename,
    q.query,
    LISTAGG(TRIM(t.text), '')
        WITHIN GROUP (ORDER BY t.sequence) AS full_sql
FROM stl_query q
JOIN stl_querytext t
    ON q.query = t.query
LEFT JOIN pg_user u
    ON q.userid = u.usesysid
WHERE q.starttime >= DATEADD(day, -7, GETDATE())
GROUP BY
    q.starttime,
    q.userid,
    u.usename,
    q.query
HAVING LOWER(
    LISTAGG(TRIM(t.text), '')
    WITHIN GROUP (ORDER BY t.sequence)
) LIKE '%delete from public.ctr%'
ORDER BY q.starttime DESC;
```

The `usename` column will tell you which Redshift user executed it. Then match the `starttime` against your Glue job runs to identify which job or schedule caused it.
