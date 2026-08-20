{{ config(materialized='table') }}

-- One row per completed unit.
--
-- The grain is deliberate and worth stating: a cycle event IS a completed unit,
-- so `count(*)` here equals units produced. If that ever stops being true the
-- reconciliation breaks loudly, which is the correct outcome.

select
    event_id            as cycle_key,
    machine_id,
    window_start,
    event_time,
    work_order,
    sku,
    unit_seq,
    duration_s,
    operator_badge,
    schema_version,
    is_late,
    publish_lag_s

from {{ ref('slv_events') }}
where event_type = 'cycle'
