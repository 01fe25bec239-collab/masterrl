-- A probe measures only the config it ran. Older adjusted aliases are no longer used.
ALTER TABLE resource_estimates DROP COLUMN adjusted_sha256;

-- Each new probe captures the calibration it used. Freeze existing rows at their current
-- calibration so upgrading does not change the ranges users already see.
ALTER TABLE resource_estimates
  ADD COLUMN calibration_n integer,
  ADD COLUMN calibration_sum_ln double precision,
  ADD COLUMN calibration_sum_ln_sq double precision;
UPDATE resource_estimates e
   SET calibration_n = c.n, calibration_sum_ln = c.sum_ln, calibration_sum_ln_sq = c.sum_ln_sq
  FROM estimator_calibration c
 WHERE e.calibration_key = c.key AND e.state = 'READY';

-- Keep the pinned estimate after the first sample so later telemetry can correct it.
ALTER TABLE estimator_calibration_pending RENAME TO estimator_calibration_inputs;
INSERT INTO estimator_calibration_inputs (attempt_id, estimate_id)
SELECT s.attempt_id, s.estimate_id
  FROM estimator_calibration_samples s
  JOIN job_attempts a ON a.id = s.attempt_id
  JOIN jobs j ON j.id = a.job_id
  JOIN run_configs c ON c.run_id = j.run_id AND c.estimate_id = s.estimate_id
ON CONFLICT (attempt_id) DO NOTHING;

CREATE VIEW estimator_calibration_measurements AS
SELECT i.attempt_id, i.estimate_id, j.org_id, j.project_id, e.estimator_version,
       e.algorithm_id, c.resolved, measured.architecture, measured.ratio
  FROM estimator_calibration_inputs i
  JOIN job_attempts a ON a.id = i.attempt_id
  JOIN jobs j ON j.id = a.job_id
  JOIN run_configs c ON c.run_id = j.run_id AND c.estimate_id = i.estimate_id
  JOIN resource_estimates e ON e.id = i.estimate_id
    AND e.org_id = j.org_id AND e.project_id = j.project_id
    AND e.probe_spec->'resolved' = c.resolved
  CROSS JOIN LATERAL (
    SELECT string_agg(DISTINCT g.vendor || ':' || g.arch, '+' ORDER BY g.vendor || ':' || g.arch)
               FILTER (WHERE btrim(g.arch) <> '') AS architecture,
           CASE WHEN e.gpu_mem_lo_gib + e.gpu_mem_hi_gib >= 2.0 / 1073741824.0
               THEN coalesce(max(ra.peak) FILTER (WHERE ra.signal = 'process_peak'),
                             max(ra.peak) FILTER (WHERE ra.signal = 'torch_reserved_peak'))
                    / 1073741824.0 / ((e.gpu_mem_lo_gib + e.gpu_mem_hi_gib) / 2.0)
           END AS ratio
      FROM resource_reservations r
      JOIN reservation_devices d ON d.reservation_id = r.id
      JOIN gpu_devices g ON g.id = d.gpu_device_id
      LEFT JOIN resource_actuals ra ON ra.attempt_id = i.attempt_id AND ra.device = g.device_uuid
        AND ra.peak >= 1 AND ra.peak < 'Infinity'::float8
        AND ra.signal IN ('process_peak', 'torch_reserved_peak')
     WHERE r.attempt_id = i.attempt_id
  ) measured
 WHERE e.state = 'READY' AND e.ok IS TRUE
   AND measured.architecture IS NOT NULL AND measured.ratio > 0 AND measured.ratio < 'Infinity'::float8;
