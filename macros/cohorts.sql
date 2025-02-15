{% macro deal_cohorts(start_date="2021-01-01", period_interval="1 month", max_delta=0) %}
  {#
   #
   #}

   {% set stages = [
       "stage_disqualified_at"
     , "stage_lost_at"
     , "stage_operation_at"
     , "stage_onboarding_at"
     , "stage_signed_at"
     , "stage_offer_sent_at"
     , "stage_hot_at"
     , "stage_warm_at"
     , "stage_lukewarm_at"
     , "stage_cold_at"
   ] %}
   {% set won_stages = ["signed", "operation", "onboarding"] %}
   {% set lost_stages = ["lost", "disqualified"] %}

  WITH deals as (

    SELECT
        id
      , created_at
      -- must coalesce dealsize_category for ROLLUP later to work properly
      , COALESCE(dealsize_category, '(none)') AS dealsize_category

      {% for stage in stages %}
        , {{ stage }}
      {% endfor %}

    FROM {{ ref("fact_deals") }}
    WHERE created_at >= '{{ start_date }}'
      AND pipeline = 'default'

  ), deal_stages_unpivoted as (
    -- This is the basis for the final analysis: for each
    -- potential cohort analysis, get all relevant deals with respective dates

    /*
     *  The following logic contains complexity due to two requirements:
     *    1. In addition to individual stages, we need to have the aggregate stages
     *        "all won", "all lost", and "all closed".
     *    2. Since a deal can be both won and lost at different times (e.g. first won,
     *        later lost), and "all closed" = "all won" + "all lost", those deals
     *        shall only be counted towards whichever happened last - yet they
     *        need to be counted towards that at the first time it happened. Otherwise,
     *        these deals would count toward both "all won" and "all lost".
     *
     */

    WITH individual_stages AS (
      -- Get an event-like structure of when a deal entered a particular stage
      {% for stage in stages %}
        SELECT
            id
          , created_at
          , dealsize_category
          , {{ stage }} AS stage_at
          , '{{ stage[6:-3] }}' AS stage_name
        FROM deals
        -- WHERE NOT {{ stage }} IS NULL
        {{ "UNION ALL" if not loop.last }}
      {% endfor %}
    ), all_closed_stages AS (
      -- filter: only stages that are won or lost (together = closed)
      SELECT
          *
        , FIRST_VALUE(stage_name) OVER w IN ('{{ won_stages|join("','") }}')
          AS is_eventually_won -- if final stage is not won, it can only be lost here
      FROM individual_stages
      WHERE stage_name IN ('{{ won_stages|join("','") }}')
         OR stage_name IN ('{{ lost_stages|join("','") }}')
      WINDOW w AS (PARTITION BY id ORDER BY stage_at DESC)
    ), all_won AS (
      -- create the aggregate stage "all won", only those deals who were eventually won
      SELECT id, created_at, dealsize_category, MIN(stage_at) AS stage_at, 'all won'
      FROM all_closed_stages
      WHERE stage_name IN ('{{ won_stages|join("','") }}')
        AND is_eventually_won
      GROUP BY 1,2,3,5
    ), all_lost AS (
      -- create the aggregate stage "all lost", only those deals who were eventually lost
      SELECT id, created_at, dealsize_category, MIN(stage_at) AS stage_at, 'all lost'
      FROM all_closed_stages
      WHERE stage_name IN ('{{ lost_stages|join("','") }}')
        AND NOT is_eventually_won
      GROUP BY 1,2,3,5
    )
      SELECT * FROM individual_stages
    UNION ALL
      SELECT * FROM all_won
    UNION ALL
      SELECT * FROM all_lost
    UNION ALL
      -- Use the fact that "all won" and "all lost" together are "all closed", yet
      -- they are mutually exclusive as one deal can only be in one of either (MECE!)
      SELECT id, created_at, dealsize_category, stage_at, 'all closed'
      FROM (SELECT * FROM all_won UNION ALL SELECT * FROM all_lost) t

  ), distinct_stages AS (

    SELECT DISTINCT stage_name, dealsize_category FROM deal_stages_unpivoted

  ), cohort_matrix AS (

    SELECT
        gs::DATE AS period
      , distinct_stages.stage_name
      , distinct_stages.dealsize_category
      , delta
      , delta + INTERVAL '{{ period_interval }}' AS delta_next
      , ROW_NUMBER() OVER w - 1 AS delta_num
    FROM GENERATE_SERIES(
          '{{ start_date }}'::DATE
        , NOW()
        , INTERVAL '{{ period_interval }}'
      ) AS gs
      CROSS JOIN GENERATE_SERIES(
            gs::DATE
          , NOW()
          , INTERVAL '{{ period_interval }}'
        ) AS delta
      CROSS JOIN distinct_stages
    WINDOW w AS (
      PARTITION BY gs, distinct_stages.stage_name, distinct_stages.dealsize_category
      ORDER BY delta ASC
    )

  ), cohort_size AS (

    WITH cm AS (
      SELECT DISTINCT
          period
        , delta
        , delta_next
        , dealsize_category
      FROM cohort_matrix
      WHERE delta_num = 0
    ), rolled_up AS (
      SELECT
          cm.period
        , cm.delta
        , cm.delta_next
        , cm.dealsize_category
        , COALESCE(COUNT(d.id), 0) AS cohort_size
      FROM cm
        LEFT JOIN deals AS d
          ON d.created_at >= cm.delta
          AND d.created_at < cm.delta_next
          AND cm.dealsize_category = d.dealsize_category
      GROUP BY 1,2,3, ROLLUP(4)
    )
    SELECT
        period
      , delta
      , delta_next
      , COALESCE(dealsize_category, 'All (aggregated)') AS dealsize_category
      , cohort_size
    FROM rolled_up

  ), cohorts AS (

    SELECT
        cm.period AS cohort_period
      , cm.delta_num AS period_delta
      , cm.stage_name AS cohort_type
      , cm.dealsize_category
      , COALESCE(COUNT(ds.id), 0) AS cohort_period_count
    FROM cohort_matrix AS cm
      LEFT JOIN cohort_size AS cs
        -- needed for created_at filter below
        ON cs.period = cm.period
        AND cs.dealsize_category = cm.dealsize_category
      LEFT JOIN deal_stages_unpivoted AS ds
        ON ds.created_at >= cs.delta
        AND ds.created_at < cs.delta_next
        AND ds.stage_at >= cm.delta
        AND ds.stage_at < cm.delta_next
        AND ds.stage_name = cm.stage_name
        AND ds.dealsize_category = cm.dealsize_category

    {% if max_delta > 0 %}
      WHERE cm.delta_num <= {{ max_delta }}
    {% endif %}
    GROUP BY 1,2,3, ROLLUP(4)

  ), add_sizes AS (

    SELECT
        co.cohort_period
      , co.period_delta
      , co.cohort_type
      , cs.dealsize_category
      , co.cohort_period_count
      , cs.cohort_size
    FROM cohorts AS co
      LEFT JOIN cohort_size AS cs
        ON cs.period = co.cohort_period
        AND cs.dealsize_category = COALESCE(co.dealsize_category, 'All (aggregated)')

  ), add_metrics AS (

    SELECT
        {{ dbt_utils.surrogate_key([
            "cohort_period"
          , "period_delta"
          , "cohort_type"
          , "dealsize_category"
        ]) }} AS _id
      , *
      , ROUND(cohort_period_count::NUMERIC / NULLIF(cohort_size::NUMERIC, 0), 4)
        AS cohort_period_count_pct
      , SUM(cohort_period_count) over w AS cohort_period_count_cumulative
      , ROUND(
            (SUM(cohort_period_count) over w)::NUMERIC / NULLIF(cohort_size::NUMERIC, 0)
          , 4
        ) AS cohort_period_count_cumulative_pct
    FROM add_sizes
    WINDOW w AS (
      PARTITION BY cohort_period, cohort_type, dealsize_category
      ORDER BY period_delta ASC
    )

  ), add_inverse_metrics AS (

    SELECT * FROM add_metrics

    UNION ALL
    SELECT
        {{ dbt_utils.surrogate_key([
            "cohort_period"
          , "period_delta"
          , "'all open'"
          , "dealsize_category"
        ]) }} AS _id
      , cohort_period
      , period_delta
      , 'all open' AS cohort_type
      , dealsize_category
      , cohort_size - cohort_period_count_cumulative
      , cohort_size
      , 1 - cohort_period_count_cumulative_pct
      , cohort_size - cohort_period_count_cumulative
      , 1 - cohort_period_count_cumulative_pct
    FROM add_metrics
    WHERE cohort_type = 'all closed'

  )

  SELECT * FROM add_inverse_metrics

{% endmacro %}
