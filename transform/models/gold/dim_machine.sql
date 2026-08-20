{{ config(materialized='table') }}

-- Machine dimension, built from what the event stream actually observed.
--
-- Deliberately NOT read from plant/canon.yaml. A dimension sourced from the
-- config file would agree with the manifest by construction and prove nothing;
-- sourcing it from the stream means a machine that stopped reporting shows up
-- as one that stopped reporting.
--
-- SCD2 arrives in Phase 3b, once the generator changes machine configs
-- mid-history. Modelling slowly-changing rows before anything changes would be
-- ceremony — there would be exactly one version of every row.

select
    machine_id,
    max(line_id) as line_id,
    min(event_time) as first_seen,
    max(event_time) as last_seen,
    count(*) as events_observed,
    count(distinct work_order) as work_orders_observed

from {{ ref('slv_events') }}
group by machine_id
