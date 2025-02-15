WITH unioned AS (

    SELECT * FROM {{ ref('interim_recurring_revenue_by_day') }}

    UNION ALL

    SELECT * FROM {{ ref('interim_churn_by_day') }}

), mrr_with_previous AS (
  -- get prior month MRR and calculate MRR change

    SELECT
      *
      , COALESCE(
          LAG(is_active) OVER (PARTITION BY hubspot_company_id ORDER BY date ASC),
          false
      ) AS previous_day_is_active
      , COALESCE(
          LAG(monthly_revenue) OVER (PARTITION BY hubspot_company_id ORDER BY date ASC),
          0
      ) AS previous_day_mrr
      , ROW_NUMBER() OVER (PARTITION BY hubspot_company_id, date ORDER BY date ASC) AS num

    FROM unioned

), mrr_with_changes AS (

   SELECT
     *
     , monthly_revenue - previous_day_mrr AS mrr_change

     FROM mrr_with_previous
     WHERE num = 1

), final AS (
  -- classify months as new, churn, reactivation, upgrade, downgrade (or none)
    SELECT
        {{  dbt_utils.surrogate_key(['date', 'hubspot_company_id']) }} AS date_company_id
        , *
        , CASE
            WHEN is_first_date
                THEN 'new'
            WHEN NOT(is_active) AND previous_day_is_active
                THEN 'churn'
            WHEN is_active AND NOT(previous_day_is_active)
                THEN 'reactivation'
            WHEN mrr_change > 0 THEN 'upgrade'
            WHEN mrr_change < 0 THEN 'downgrade'
            WHEN mrr_change = 0 THEN 'none'
          END AS change_category

    FROM mrr_with_changes

)

SELECT * FROM final
