Some `NULL`s are expected, but two columns need checking.

Expected `NULL`s:

* `calibration_session_id`: expected because these rows are `STANDARD`, not calibration evaluations.
* Acknowledgement fields: expected until someone acknowledges the evaluation.
* `evaluated_participant_id` and role: older/manual evaluations may not contain them. Newer automated records in your output do have them.
* A missing score can be valid only if the evaluation form had no scoring configured or the evaluation was marked not applicable.

Concerning:

* `evaluation_score_percentage` is `NULL` for every displayed record. Your script reads `metadata.score.percentage`, which is the correct AWS structure. AWS’s official sample also places it there. Therefore we need to determine whether the source JSON actually lacks the score or whether the Glue schema failed to expose it. [AWS Contact Evaluation output schema](https://docs.aws.amazon.com/connect/latest/adminguide/evaluationforms-example-output-file.html)
* `source_file` is `NULL` for every row. The script does not generate it; it only preserves it if the preprocessing job already added it. This means the preprocessing output/Catalog table doesn’t contain `source_file`. It does not mean evaluation data is missing, but you lose file-level traceability.

Run this first to inspect the score inside the retained `metadata` SUPER column:

```sql
SELECT
    evaluation_id,
    evaluation_definition_title,
    metadata.score AS score_object,
    metadata.score.percentage::DOUBLE PRECISION AS score_percentage,
    evaluation_score_percentage
FROM public.contact_evaluations
LIMIT 20;
```

Then get a proper null summary:

```sql
SELECT
    COUNT(*) AS total_records,
    SUM(CASE WHEN evaluation_score_percentage IS NULL THEN 1 ELSE 0 END)
        AS null_score_count,
    SUM(CASE WHEN calibration_session_id IS NULL THEN 1 ELSE 0 END)
        AS null_calibration_count,
    SUM(CASE WHEN evaluated_participant_id IS NULL THEN 1 ELSE 0 END)
        AS null_participant_count,
    SUM(CASE WHEN evaluation_acknowledged_timestamp IS NULL THEN 1 ELSE 0 END)
        AS null_acknowledgement_count,
    SUM(CASE WHEN source_file IS NULL THEN 1 ELSE 0 END)
        AS null_source_file_count
FROM public.contact_evaluations;
```

Important: don’t change the script yet. If `metadata.score.percentage` returns values while `evaluation_score_percentage` remains `NULL`, then the extraction is the problem. If both are `NULL`, the source evaluations simply do not contain a form-level score.

Also, your current `NOT EXISTS` insert will not correct already-loaded evaluation IDs after a fix. Existing rows would need an `UPDATE`/`MERGE` or a controlled reload.
