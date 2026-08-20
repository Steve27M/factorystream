{{ config(materialized='table') }}

-- One row per defect. Joins to fct_cycles on (work_order, unit_seq) — the
-- natural key of a produced unit — rather than on event_id, because a defect is
-- a separate event about the same unit.

select
    event_id            as defect_key,
    machine_id,
    window_start,
    event_time,
    work_order,
    sku,
    unit_seq,
    defect_code,
    operator_badge as inspector,
    is_late

from {{ ref('slv_events') }}
where event_type = 'defect'
