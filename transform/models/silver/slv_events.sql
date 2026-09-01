{{ config(materialized='table') }}

-- Deduplicate on event_id, keeping the earliest publish.
--
-- The injector republishes ~1% of events 1–3 extra times, byte-identical apart
-- from publish_time — exactly what a broker retry produces. Keeping the
-- earliest copy is the meaningful choice: it is the one that actually arrived
-- first, so `publish_lag_s` stays honest rather than reflecting a retry.
--
-- `row_number()` rather than `select distinct`: distinct would collapse rows
-- that differ only in publish_time into an arbitrary survivor, and would give
-- no way to count what was removed.

with ranked as (

    select
        *,
        row_number() over (
            partition by event_id
            order by publish_time asc, kafka_offset asc
        ) as copy_rank

    from {{ ref('stg_events') }}

),

deduped as (

    select * from ranked where copy_rank = 1

)

select
    event_id,
    event_type,
    machine_id,
    event_time,
    publish_time,
    schema_version,
    operator_badge,
    line_id,
    work_order,
    sku,
    unit_seq,
    duration_s,
    defect_code,
    from_state,
    to_state,
    reason,
    scan_action,
    badge_id,
    ingest_ts,
    kafka_partition,
    kafka_offset,
    publish_lag_s,

    -- The 15-minute event-time window. Computed in EVENT time, including any
    -- clock skew — the manifest records what the machine reported, so grading
    -- against a corrected time the pipeline never saw would fail it for the
    -- generator's fiction.
    {{ window_start("event_time") }} as window_start,

    -- Flagged, not filtered. A late arrival is real data that needs
    -- re-statement, and dropping it would be the silent-loss failure this
    -- project exists to make impossible.
    publish_lag_s > 60 as is_late

from deduped
