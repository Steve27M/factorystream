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
        {{ json_string('payload', 'operator') }},
        {{ json_string('payload', 'operator_badge') }}
    ) as operator_badge,

    -- Added in v2, absent in v1. Null is the honest answer for v1 rows, not a
    -- backfilled guess.
    {{ json_string('payload', 'line_id') }} as line_id,

    {{ json_string('payload', 'work_order') }} as work_order,
    {{ json_string('payload', 'sku') }} as sku,
    cast({{ json_string('payload', 'unit_seq') }} as integer) as unit_seq,
    -- v3 renamed `duration_s` (seconds, float) to `duration_ms` (milliseconds,
    -- integer). Unlike the v1->v2 rename this is a UNIT change as well, so
    -- conforming it is a division and not just a coalesce - and getting that
    -- wrong produces numbers 1000x too large that are otherwise perfectly
    -- plausible.
    --
    -- Seconds is the conformed unit because silver, gold and the manifest all
    -- speak it, and moving the warehouse to milliseconds to follow the newest
    -- producer would rewrite every downstream comparison.
    coalesce(
        cast({{ json_string('payload', 'duration_s') }} as double),
        cast({{ json_string('payload', 'duration_ms') }} as double) / 1000.0
    ) as duration_s,
    {{ json_string('payload', 'defect_code') }} as defect_code,
    {{ json_string('payload', 'from_state') }} as from_state,
    {{ json_string('payload', 'to_state') }} as to_state,
    {{ json_string('payload', 'reason') }} as reason,
    {{ json_string('payload', 'action') }} as scan_action,
    {{ json_string('payload', 'badge_id') }} as badge_id,

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
