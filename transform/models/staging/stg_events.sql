{{ config(materialized='view') }}

-- Conform both schema versions into one shape.
--
-- The v1 -> v2 cutover renamed `operator` to `operator_badge` on cycle events
-- and added `line_id`. An added field is survivable by any reader that ignores
-- unknown keys; **a renamed field is not** — a reader assuming the old name
-- silently produces nulls rather than failing, which is why the injector
-- includes one.
--
-- Conforming here means nothing downstream ever sees the drift. That is the
-- whole point of a staging layer, and it is why `bronze` keeps the payload raw:
-- silver cannot conform from something already conformed.

select
    event_id,
    event_type,
    machine_id,
    event_time,
    publish_time,
    schema_version,

    -- The renamed field, unified. coalesce order matters only for readability;
    -- exactly one is ever present.
    coalesce(
        json_extract_scalar(payload, '$.operator'),
        json_extract_scalar(payload, '$.operator_badge')
    ) as operator_badge,

    -- Added in v2, absent in v1. Null is the honest answer for v1 rows, not a
    -- backfilled guess.
    json_extract_scalar(payload, '$.line_id') as line_id,

    json_extract_scalar(payload, '$.work_order') as work_order,
    json_extract_scalar(payload, '$.sku') as sku,
    cast(json_extract_scalar(payload, '$.unit_seq') as integer) as unit_seq,
    cast(json_extract_scalar(payload, '$.duration_s') as double) as duration_s,
    json_extract_scalar(payload, '$.defect_code') as defect_code,
    json_extract_scalar(payload, '$.from_state') as from_state,
    json_extract_scalar(payload, '$.to_state') as to_state,
    json_extract_scalar(payload, '$.reason') as reason,
    json_extract_scalar(payload, '$.action') as scan_action,
    json_extract_scalar(payload, '$.badge_id') as badge_id,

    ingest_ts,
    kafka_partition,
    kafka_offset,

    -- Event time vs ingest time, kept separate end to end. Coalescing these
    -- into one "timestamp" column would destroy the late-arrival and
    -- clock-skew narratives in a single edit.
    date_diff('second', event_time, publish_time) as publish_lag_s,

    dt,
    hr

from {{ source('lake', 'bronze_events') }}
