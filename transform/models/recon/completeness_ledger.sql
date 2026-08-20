{{ config(materialized='table') }}

-- The headline artifact: one row per window per machine, stating whether the
-- warehouse reproduces generator ground truth exactly.
--
-- Real pipelines assert correctness; almost none can prove it, because ground
-- truth is unknowable once data leaves the source. A synthetic generator knows
-- exactly what it emitted, writes it to `manifests` **without touching the
-- pipeline**, and this model checks the equality.
--
-- The expected identities, and why each disorder class shifts them:
--
--   deduped events in gold  ==  manifest event_count - corrupt_count
--                               (corrupt payloads are quarantined, not lost)
--
-- Duplicates do not appear here at all: silver removed them, which is the
-- point. `duplicate_extra_count` is carried through so the ledger can assert
-- the removal was exactly right rather than merely plausible.
--
-- Three states, and the distinction between the last two matters:
--   reconciled — deltas are zero. The claim holds.
--   pending    — inside the watermark; late data may still arrive. NOT a failure.
--   broken     — past the watermark and still divergent. Fails the build.
--
-- A ledger without `pending` would either fire constantly on fresh windows or
-- suppress real breaks by widening its tolerance. Separating "too early to
-- tell" from "wrong" is what makes the gate trustworthy enough to leave on.

with manifest as (

    select
        machine_id,
        window_start,
        event_count,
        cycle_count,
        defect_count,
        state_change_count,
        operator_scan_count,
        unit_count,
        cycle_duration_sum_s,
        event_id_checksum,
        corrupt_count,
        duplicate_extra_count,
        late_count,
        corrupt_cycle_count,
        corrupt_defect_count,
        corrupt_duration_sum_s
    from {{ source('lake', 'manifests') }}

),

observed as (

    select * from {{ ref('agg_window_machine') }}

),

-- FULL OUTER, not inner. An inner join silently drops the two failures that
-- matter most: a window the pipeline lost entirely, and a window the pipeline
-- invented. Both must surface as broken, not vanish from the report.
joined as (

    select
        coalesce(m.machine_id, o.machine_id)     as machine_id,
        coalesce(m.window_start, o.window_start) as window_start,

        m.machine_id is null                     as missing_from_manifest,
        o.machine_id is null                     as missing_from_gold,

        -- Expected values: what gold SHOULD hold, given what was injected.
        --
        -- Each measure gets its OWN corruption adjustment. Using the total
        -- `corrupt_count` for every measure was wrong and the ledger caught it:
        -- four windows reported `unit_delta = -1` while `event_delta = 0`,
        -- because the corrupted events happened to be cycles and the expected
        -- unit count had not been reduced. A corrupted cycle removes one event,
        -- one unit, AND its duration from what the warehouse can observe.
        coalesce(m.event_count, 0) - coalesce(m.corrupt_count, 0)
                                                 as expected_event_count,
        coalesce(m.cycle_count, 0) - coalesce(m.corrupt_cycle_count, 0)
                                                 as expected_cycle_count,
        coalesce(m.defect_count, 0) - coalesce(m.corrupt_defect_count, 0)
                                                 as expected_defect_count,
        coalesce(m.unit_count, 0) - coalesce(m.corrupt_cycle_count, 0)
                                                 as expected_unit_count,
        coalesce(m.cycle_duration_sum_s, 0.0) - coalesce(m.corrupt_duration_sum_s, 0.0)
                                                 as expected_duration_sum,

        coalesce(o.event_count, 0)               as observed_event_count,
        coalesce(o.cycle_count, 0)               as observed_cycle_count,
        coalesce(o.defect_count, 0)              as observed_defect_count,
        coalesce(o.unit_count, 0)                as observed_unit_count,
        coalesce(o.cycle_duration_sum_s, 0.0)    as observed_duration_sum,

        coalesce(m.corrupt_count, 0)             as injected_corrupt,
        coalesce(m.corrupt_cycle_count, 0)       as injected_corrupt_cycles,
        coalesce(m.duplicate_extra_count, 0)     as injected_duplicate_extra,
        coalesce(m.late_count, 0)                as injected_late,
        coalesce(o.late_observed, 0)             as observed_late,

        m.event_id_checksum

    from manifest m
    full outer join observed o
        on  m.machine_id   = o.machine_id
        and m.window_start = o.window_start

),

deltas as (

    select
        *,
        observed_event_count  - expected_event_count   as event_delta,
        observed_unit_count   - expected_unit_count    as unit_delta,
        observed_defect_count - expected_defect_count  as defect_delta,
        observed_duration_sum - expected_duration_sum  as duration_delta,

        date_diff('hour', window_start, current_timestamp) as window_age_hours

    from joined

)

select
    machine_id,
    window_start,
    window_age_hours,

    expected_event_count,
    observed_event_count,
    event_delta,
    unit_delta,
    defect_delta,
    -- Absolute tolerance rather than exact equality: the sum crosses Python,
    -- Athena and DuckDB, which will not agree at the 15th digit. 0.01s against
    -- cycle times of ~100s is far tighter than any real discrepancy.
    duration_delta,

    injected_corrupt,
    injected_corrupt_cycles,
    injected_duplicate_extra,
    injected_late,
    observed_late,

    missing_from_manifest,
    missing_from_gold,

    case
        when missing_from_manifest then 'broken'
        when missing_from_gold     then 'broken'
        when event_delta = 0
         and unit_delta = 0
         and defect_delta = 0
         and abs(duration_delta) < 0.01
            then 'reconciled'
        -- Inside the watermark, late data may still arrive. Not a failure yet.
        when window_age_hours < {{ var('watermark_hours') }} then 'pending'
        else 'broken'
    end as status,

    case
        when missing_from_manifest
            then 'window present in gold but absent from ground truth — the pipeline invented data'
        when missing_from_gold
            then 'window present in ground truth but absent from gold — the pipeline lost a window'
        when event_delta <> 0
            then 'event count differs after accounting for injected corruptions'
        when unit_delta <> 0 then 'unit count differs'
        when defect_delta <> 0 then 'defect count differs'
        when abs(duration_delta) >= 0.01 then 'cycle duration sum differs'
    end as root_cause

from deltas
