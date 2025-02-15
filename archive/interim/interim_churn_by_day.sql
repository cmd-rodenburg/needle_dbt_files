WITH mrr AS (

    SELECT * FROM {{ ref('interim_recurring_revenue_by_day') }}

), final AS (
   -- row for day *after* last day of activity

    SELECT
       date + INTERVAL '1 day' AS date
       , hubspot_company_id
       , CAST(0 AS FLOAT) AS monthly_revenue
       , false AS is_active
       , first_active_date
       , last_active_date
       , false AS is_first_date
       , false AS is_last_date
       , date_customer_id

    FROM mrr
    WHERE is_last_date = true
    AND date < CURRENT_DATE
)

SELECT * FROM final
