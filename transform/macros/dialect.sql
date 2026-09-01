{#
    Cross-engine shims, so one model set runs on Athena and on DuckDB.

    **Why a second engine at all.** The Athena target was the only target, which
    meant dbt could not run in CI at all — no credentials on a runner — and the
    entire warehouse layer went unexercised on every push. A model set nobody
    can execute is a model set nobody can check, and this project's whole claim
    is that its numbers are checkable.

    A DuckDB target over the same parquet closes that. It is not a mock: DuckDB
    reads the identical objects the consumer wrote, so a disagreement between
    the two engines is a real disagreement about the data rather than a fixture
    drifting from production.

    **There is exactly one shim, because exactly one function differs.**
    `date_diff('second', a, b)` was wrapped first on the assumption that DuckDB
    wanted a bare identifier for the unit; it does not, and the string form is
    accepted verbatim by both. The wrapper was deleted rather than kept "for
    symmetry" — a shim that wraps nothing is a private dialect with no
    justification, and the next person has to read it to discover that.

    Checked rather than assumed, which is how the wrong version was caught:

        json_extract_scalar   Athena only; DuckDB raises a Catalog Error
        json_extract          exists in both, returns `"x"` WITH quotes in
                              DuckDB, so a cast silently produces nulls
        json_extract_string   DuckDB's unquoted equivalent — the right one

    `adapter.dispatch` resolves `<adapter>__name` first and falls back to
    `default__name`. Athena takes the default, because these models were written
    against it and the diff should show what DuckDB needed rather than
    re-parenting the original.
#}

{% macro json_string(column, key) %}
  {{ return(adapter.dispatch('json_string', 'factorystream')(column, key)) }}
{% endmacro %}

{% macro default__json_string(column, key) %}
  json_extract_scalar({{ column }}, '$.{{ key }}')
{% endmacro %}

{% macro duckdb__json_string(column, key) %}
  {#- DuckDB's equivalent returns an unquoted VARCHAR; `json_extract` would
      return a JSON value with the quotes still on, which then casts wrong. -#}
  json_extract_string({{ column }}, '$.{{ key }}')
{% endmacro %}


{#
    Truncate a timestamp down to a fixed-width window.

    A second genuine difference, found the same way as the first: by running it.
    Athena spells the round trip `from_unixtime` / `to_unixtime`; DuckDB spells
    it `to_timestamp` / `epoch`, and has neither of Athena's names.

    Not `date_trunc`, which only takes named units - there is no 'quarter hour',
    and expressing 15 minutes through it would mean truncating to the hour and
    adding back a computed remainder, which is more arithmetic in more places
    for a worse result.
#}

{% macro window_start(ts, seconds=900) %}
  {{ return(adapter.dispatch('window_start', 'factorystream')(ts, seconds)) }}
{% endmacro %}

{% macro default__window_start(ts, seconds) %}
  from_unixtime(floor(to_unixtime({{ ts }}) / {{ seconds }}) * {{ seconds }})
{% endmacro %}

{% macro duckdb__window_start(ts, seconds) %}
  to_timestamp(floor(epoch({{ ts }}) / {{ seconds }}) * {{ seconds }})
{% endmacro %}
