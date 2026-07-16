The screenshots show exactly what happened:

```sql
DROP TABLE IF EXISTS public.ctr;
```

It ran at approximately:

```text
2026-07-16 04:39:59
```

with transaction ID:

```text
327016723
```

So `public.ctr` was **dropped**, not merely truncated. Afterward, the recurring process ran statements such as:

```sql
CREATE TABLE IF NOT EXISTS public.ctr (...)
```

That recreated the table structure, but it did not restore the old rows. That explains why the table exists now but returns zero rows.

Reading from the table did not cause this. An explicit `DROP TABLE public.ctr` statement did.

Run this to identify the Redshift user and full statement associated with that transaction:

```sql
SELECT
    q.starttime,
    q.endtime,
    q.xid,
    q.query,
    q.userid,
    u.usename,
    q.aborted,
    LISTAGG(TRIM(t.text), '')
        WITHIN GROUP (ORDER BY t.sequence) AS full_sql
FROM stl_query q
JOIN stl_querytext t
    ON q.query = t.query
LEFT JOIN pg_user u
    ON q.userid = u.usesysid
WHERE q.xid = 327016723
GROUP BY
    q.starttime,
    q.endtime,
    q.xid,
    q.query,
    q.userid,
    u.usename,
    q.aborted
ORDER BY q.starttime;
```

Also reconstruct all DDL statements from that transaction:

```sql
SELECT
    starttime,
    xid,
    LISTAGG(TRIM(text), '')
        WITHIN GROUP (ORDER BY sequence) AS full_ddl
FROM stl_ddltext
WHERE xid = 327016723
GROUP BY starttime, xid
ORDER BY starttime;
```

To see which application or process opened the session, use the query ID returned above:

```sql
SELECT
    q.query,
    q.starttime,
    q.userid,
    u.usename,
    q.pid,
    s.remotehost,
    s.remoteport,
    s.application_name
FROM stl_query q
LEFT JOIN pg_user u
    ON q.userid = u.usesysid
LEFT JOIN stl_sessions s
    ON q.pid = s.process
WHERE q.xid = 327016723
ORDER BY q.starttime;
```

The key finding is already confirmed: **`public.ctr` was dropped at 04:39:59 and then recreated empty.** The next step is to use the transaction query above to confirm whether the drop came from Query Editor, a Glue connection, a scheduled process, or another database user.
