CREATE TABLE IF NOT EXISTS horsies_recovery_scan_cursors (
    scan_name varchar(64) PRIMARY KEY,
    last_id uuid,
    completed_cycles bigint NOT NULL DEFAULT 0,
    last_scan_rows integer NOT NULL DEFAULT 0,
    last_candidate_rows integer NOT NULL DEFAULT 0,
    last_scan_at timestamptz
);

INSERT INTO horsies_recovery_scan_cursors (scan_name)
VALUES ('running_workflows'), ('orphan_workflow_tasks')
ON CONFLICT (scan_name) DO NOTHING;

CREATE INDEX IF NOT EXISTS idx_horsies_workflows_running_recovery_scan
    ON horsies_workflows (id) INCLUDE (name)
    WHERE status = 'RUNNING';

CREATE INDEX IF NOT EXISTS idx_horsies_tasks_orphan_recovery_scan
    ON horsies_tasks (id)
    WHERE is_workflow_task = TRUE
      AND status IN ('CLAIMED', 'PENDING');

CREATE OR REPLACE FUNCTION horsies_cancel_orphaned_tasks(
    p_batch_size integer
)
RETURNS SETOF horsies_terminalization_outcome
LANGUAGE plpgsql
AS $$
DECLARE
    v_cursor uuid;
    v_scan_ids uuid[];
    v_wrap_ids uuid[];
    v_ids uuid[];
    v_scan_count integer;
    v_wrapped boolean := FALSE;
    v_terminal_at timestamptz;
    v_moved bigint;
    v_deleted bigint;
    v_result_payload bytea;
BEGIN
    IF p_batch_size IS NULL OR p_batch_size <= 0 THEN
        RAISE EXCEPTION
            'p_batch_size must be a positive integer, got %', p_batch_size
            USING ERRCODE = 'invalid_parameter_value';
    END IF;
    PERFORM horsies_assert_archive_available();

    SELECT c.last_id INTO v_cursor
    FROM horsies_recovery_scan_cursors c
    WHERE c.scan_name = 'orphan_workflow_tasks'
    FOR UPDATE NOWAIT;
    IF NOT FOUND THEN
        RAISE EXCEPTION 'orphan workflow-task scan cursor is absent'
            USING ERRCODE = 'data_corrupted';
    END IF;

    SELECT array_agg(s.id ORDER BY s.id) INTO v_scan_ids
    FROM (
        SELECT t.id
        FROM horsies_tasks t
        WHERE t.is_workflow_task = TRUE
          AND t.status IN ('CLAIMED', 'PENDING')
          AND (v_cursor IS NULL OR t.id > v_cursor)
        ORDER BY t.id
        LIMIT p_batch_size
    ) s;
    v_scan_count := COALESCE(cardinality(v_scan_ids), 0);

    IF v_cursor IS NOT NULL AND v_scan_count < p_batch_size THEN
        SELECT array_agg(s.id ORDER BY s.id) INTO v_wrap_ids
        FROM (
            SELECT t.id
            FROM horsies_tasks t
            WHERE t.is_workflow_task = TRUE
              AND t.status IN ('CLAIMED', 'PENDING')
              AND t.id <= v_cursor
            ORDER BY t.id
            LIMIT p_batch_size - v_scan_count
        ) s;
        v_scan_ids := COALESCE(v_scan_ids, '{}'::uuid[])
            || COALESCE(v_wrap_ids, '{}'::uuid[]);
        v_scan_count := cardinality(v_scan_ids);
        v_wrapped := TRUE;
    END IF;

    SELECT array_agg(s.id ORDER BY s.id) INTO v_ids
    FROM (
        SELECT candidate.id
        FROM unnest(COALESCE(v_scan_ids, '{}'::uuid[])) AS scanned(id)
        CROSS JOIN LATERAL (
            SELECT t.id
            FROM horsies_tasks t
            LEFT JOIN LATERAL (
                SELECT TRUE AS found
                FROM horsies_workflow_tasks wt
                WHERE wt.task_id = t.id
                  AND wt.status IN ('ENQUEUED', 'READY', 'PENDING', 'RUNNING')
                LIMIT 1
            ) runnable_link ON TRUE
            WHERE t.id = scanned.id
              AND t.is_workflow_task = TRUE
              AND t.status IN ('CLAIMED', 'PENDING')
              AND runnable_link.found IS NULL
            LIMIT 1
            FOR UPDATE OF t SKIP LOCKED
        ) candidate
    ) s;

    UPDATE horsies_recovery_scan_cursors
    SET last_id = CASE
            WHEN v_scan_count = 0 THEN NULL
            ELSE v_scan_ids[v_scan_count]
        END,
        completed_cycles = completed_cycles + CASE WHEN v_wrapped THEN 1 ELSE 0 END,
        last_scan_rows = v_scan_count,
        last_candidate_rows = COALESCE(cardinality(v_ids), 0),
        last_scan_at = statement_timestamp()
    WHERE scan_name = 'orphan_workflow_tasks';

    IF v_ids IS NULL THEN
        RETURN;
    END IF;

    IF EXISTS (
        SELECT 1 FROM horsies_tasks t
        WHERE t.id = ANY(v_ids)
          AND t.is_workflow_task
          AND (SELECT count(DISTINCT wt.workflow_id)
               FROM horsies_workflow_tasks wt
               WHERE wt.task_id = t.id) > 1
    ) THEN
        RAISE EXCEPTION 'task links to multiple workflows'
            USING ERRCODE = 'data_corrupted';
    END IF;

    IF EXISTS (
        SELECT 1 FROM unnest(v_ids) AS u(tid)
        WHERE (SELECT prov.found
               FROM horsies_task_provenance_staged(u.tid, FALSE) AS prov)
    ) THEN
        RAISE EXCEPTION 'task identity exists in multiple locations'
            USING ERRCODE = 'data_corrupted';
    END IF;

    v_terminal_at := NOW();
    v_result_payload := CASE
        WHEN (NULL::text) IS NULL THEN NULL
        ELSE convert_to((NULL::text), 'UTF8')
    END;

    INSERT INTO horsies_task_history (
        task_id,
        task_name,
        queue_name,
        priority,
        command_fingerprint_version,
        command_fingerprint,
        status,
        terminalization_kind,
        terminal_at,
        retention_anchor_at,
        retention_class_key,
        sent_at,
        enqueued_at,
        claimed_at,
        started_at,
        created_at,
        good_until,
        result_envelope_version,
        result_codec,
        result_content_type,
        result_payload,
        prior_result_payload,
        result_digest,
        error_code,
        final_failed_reason,
        retry_count,
        max_retries,
        last_claimed_worker_id,
        last_worker_hostname,
        last_worker_pid,
        last_worker_process_name,
        input_digest,
        rerun_of_task_id,
        rerun_root_task_id,
        workflow_id,
        is_workflow_task,
        history_schema_version,
        attempt_archive_version,
        attempt_snapshot_codec,
        attempt_snapshot_content_type,
        attempt_snapshot,
        attempt_snapshot_digest,
        rerun_input_disposition,
        rerun_input_version,
        rerun_input_codec,
        rerun_input_content_type,
        rerun_input_digest,
        rerun_input_inline,
        rerun_input_reference
    )
    SELECT
        t.id,
        t.task_name,
        t.queue_name,
        t.priority,
        t.command_fingerprint_version,
        t.command_fingerprint,
        'CANCELLED',
        'CANCEL_ORPHAN_SWEEP',
        v_terminal_at,
        v_terminal_at,
        t.retention_class_key,
        t.sent_at,
        t.enqueued_at,
        t.claimed_at,
        t.started_at,
        t.created_at,
        t.good_until,
        1,
        'json-utf8',
        'application/json',
        v_result_payload,
        NULL,
        CASE WHEN v_result_payload IS NULL THEN NULL
             ELSE sha256(v_result_payload) END,
        'WORKFLOW_CHECK_FAILED',
        'Workflow task orphaned: no live workflow_task linkage',
        t.retry_count,
        t.max_retries,
        t.claimed_by_worker_id,
        t.worker_hostname,
        t.worker_pid,
        t.worker_process_name,
        t.input_digest,
        t.rerun_of_task_id,
        t.rerun_root_task_id,
        CASE WHEN t.is_workflow_task THEN n.workflow_id END,
        t.is_workflow_task,
        1,
        1,
        'json-utf8',
        'application/json',
        horsies_encode_task_attempts(t.id),
        sha256(horsies_encode_task_attempts(t.id)),
        d.disposition,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_version END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_codec END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_content_type END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_digest END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_inline END,
        CASE WHEN d.disposition IN ('INLINE', 'REFERENCE')
             THEN t.prepared_rerun_input_reference END
    FROM horsies_tasks t
    LEFT JOIN LATERAL (
        SELECT wt.workflow_id
        FROM horsies_workflow_tasks wt
        WHERE wt.task_id = t.id
        ORDER BY wt.id
        LIMIT 1
    ) n ON TRUE
    CROSS JOIN LATERAL (
        SELECT CASE
            WHEN t.is_workflow_task OR 'CANCELLED' = 'COMPLETED' THEN 'NEVER_ELIGIBLE'
            WHEN NOT t.retain_rerun_input THEN 'DECLINED_BY_POLICY'
            ELSE t.prepared_rerun_input_disposition
        END AS disposition
    ) d
    WHERE t.id = ANY(v_ids);
    GET DIAGNOSTICS v_moved = ROW_COUNT;
    IF v_moved <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch history insert moved % of % rows',
            v_moved, cardinality(v_ids);
    END IF;

    PERFORM horsies_key_reservation_terminalize_batch(
        (SELECT COALESCE(array_agg(t.idempotency_key_digest), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        (SELECT COALESCE(array_agg(t.id), '{}')
         FROM horsies_tasks t
         WHERE t.id = ANY(v_ids)
           AND t.idempotency_key_digest IS NOT NULL),
        v_terminal_at
    );

    RETURN QUERY SELECT
        t.id, NULL::bigint, 'APPLIED'::text,
        v_terminal_at, 'CANCEL_ORPHAN_SWEEP'::text,
        t.status::text, t.claimed_by_worker_id::varchar, t.claimed_at,
        NULL::text, NULL::jsonb
    FROM horsies_tasks t
    WHERE t.id = ANY(v_ids);

    DELETE FROM horsies_task_attempts WHERE task_id = ANY(v_ids);
    DELETE FROM horsies_tasks WHERE id = ANY(v_ids);
    GET DIAGNOSTICS v_deleted = ROW_COUNT;
    IF v_deleted <> cardinality(v_ids) THEN
        RAISE EXCEPTION 'batch live delete removed % of % rows',
            v_deleted, cardinality(v_ids);
    END IF;

    PERFORM pg_notify('task_done', u.tid::text)
    FROM unnest(v_ids) AS u(tid);
END;
$$;
