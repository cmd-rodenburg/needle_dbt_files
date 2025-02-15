{% macro get_current_table_from_snap_base(base_relation) %}

  {{ select_star_except(base_relation, ["valid_from", "valid_to", "is_current"]) }}
  WHERE is_current

{% endmacro %}

{% macro get_daily_table_from_snap_base(base_relation, pk) %}

  {% set columns_list %}
    {{ select_star_except(base_relation, ["valid_from", "valid_to", "is_current"], False) }}
  {% endset %}

    WITH raw AS (
      SELECT
        valid_from::DATE,
        COALESCE(valid_to::DATE - INTERVAL '1 day', NOW()::DATE) AS valid_to,
        MAX(COALESCE(valid_to::DATE - INTERVAL '1 day', NOW()::DATE))
          OVER w AS max_valid_to,
        {{ columns_list }}
      FROM {{ base_relation }}
      WINDOW w AS (PARTITION BY {{ pk }})
    )
    SELECT
      -- composite primary key
      {{ dbt_utils.surrogate_key([pk, 'gs::DATE']) }} AS snap_id,
      gs::DATE AS valid_date,
      gs::DATE = max_valid_to AS is_current,
      {{ columns_list }}
    FROM raw, GENERATE_SERIES(valid_from, valid_to, INTERVAL '1 day') AS gs

{% endmacro %}
