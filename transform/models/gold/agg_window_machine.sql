{{ config(materialized='table') }}

-- Window aggregate, grained to match the generator's manifest exactly.
--
-- This is the model the completeness ledger compares against ground truth, so
-- its grain is not a modelling preference — it is dictated by the manifest.
-- Any divergence in grain would make the comparison meaningless rather than
-- merely wrong.

select
    machine_id,
    window_start,

    count(*)                                          as event_count,
    count_if(event_type = 'cycle')                    as cycle_count,
    count_if(event_type = 'defect')                   as defect_count,
    count_if(event_type = 'state_change')             as state_change_count,
    count_if(event_type = 'operator_scan')            as operator_scan_count,
    count_if(event_type = 'cycle')                    as unit_count,

    -- Rounded to 3dp because float sums are compared across engines. Python,
    -- Athena and DuckDB will not agree at the 15th digit, and a reconciliation
    -- that fails on floating-point noise teaches nobody anything.
    round(sum(case when event_type = 'cycle' then duration_s end), 3)
                                                      as cycle_duration_sum_s,

    count_if(is_late)                                 as late_observed,
    max(publish_lag_s)                                as max_publish_lag_s

from {{ ref('slv_events') }}
group by machine_id, window_start
