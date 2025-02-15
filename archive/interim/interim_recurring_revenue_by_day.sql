WITH subscription_periods AS (

    SELECT * FROM {{ ref('fact_subscriptions') }}

), dates AS (

  SELECT date FROM {{ ref('dim_dates') }}

), customers AS (

   SELECT
      hubspot_company_id
      , MIN(contract_start_date) AS date_start
      , MAX(COALESCE(contract_end_date, CURRENT_DATE )) AS date_end

    FROM subscription_periods
    GROUP BY 1

), customer_days as (

    SELECT
        customers.hubspot_company_id
        , dates.date

    FROM customers
      CROSS JOIN dates
    -- all dates after start date
    WHERE dates.date >= customers.date_start
    -- and before end date or today if the subscription is active
      AND dates.date <= customers.date_end

), joined AS (

    SELECT
       customer_days.date
       , customer_days.hubspot_company_id
       , COALESCE(SUM(subscription_periods.monthly_amount), 0) AS monthly_revenue

    FROM customer_days
      LEFT JOIN subscription_periods
        ON customer_days.hubspot_company_id = subscription_periods.hubspot_company_id
        -- date is after a subscription start date
        AND customer_days.date >= subscription_periods.contract_start_date
        -- date is before a subscription end date (and handle null case)
        AND (
            (customer_days.date < subscription_periods.contract_end_date)
            OR (subscription_periods.contract_end_date IS NULL)
            )
            
    GROUP BY 1,2

), flag_added AS (

    SELECT
        *
        , monthly_revenue > 0 AS is_active

    FROM joined

), first_last_dates_added AS (

    SELECT
        *
        , MIN(CASE WHEN is_active THEN date END) OVER (
            PARTITION BY hubspot_company_id
        ) AS first_active_date
        , MAX(CASE WHEN is_active THEN date END) OVER (
            PARTITION BY hubspot_company_id
        ) AS last_active_date

    FROM flag_added

), final AS (

    SELECT
        *
        , first_active_date = date AS is_first_date
        , last_active_date = date AS is_last_date
        , {{ dbt_utils.surrogate_key(['hubspot_company_id', 'date']) }}
          AS date_customer_id

    FROM first_last_dates_added

)

SELECT * FROM final
