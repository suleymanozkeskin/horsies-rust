-- no-transaction
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_horsies_workflows_running_recovery_scan
    ON horsies_workflows (created_at, id) INCLUDE (name)
    WHERE status = 'RUNNING';
