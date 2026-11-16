-- 0007_job_cancel_permissions.sql: Grants built-in job cancellation permissions
-- per implementation decision I-10 (ADR-0006 §4, ADR-0017 §2).
-- Forward-only additive migration (ADR-0018 §2).

INSERT INTO role_permissions (role_id, permission)
SELECT r.id, v.permission
FROM (
    VALUES
        ('019318a0-0000-7000-8000-000000000001', 'jobs:cancel_own'),
        ('019318a0-0000-7000-8000-000000000001', 'jobs:cancel_all'),
        ('019318a0-0000-7000-8000-000000000002', 'jobs:cancel_own'),
        ('019318a0-0000-7000-8000-000000000003', 'jobs:cancel_own'),
        ('019318a0-0000-7000-8000-000000000004', 'jobs:cancel_own')
) AS v(role_id, permission)
JOIN roles r ON r.id = v.role_id
ON CONFLICT (role_id, permission) DO NOTHING;
