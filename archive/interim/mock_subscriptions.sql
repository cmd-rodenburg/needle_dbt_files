SELECT
  1 AS subscription_id
, 1 AS hubspot_company_id
, CAST('2021-12-13' AS DATE) AS contract_start_date
, CAST('2021-12-14' AS DATE) AS contract_end_date
, 15.0 AS monthly_amount

UNION ALL

SELECT
  2 AS subscription_id
, 1 AS hubspot_company_id
, CAST('2021-12-13' AS DATE) AS contract_start_date
, CAST('2021-12-15' AS DATE) AS contract_end_date
, 20.0 AS monthly_amount

UNION ALL

SELECT
  3 AS subscription_id
, 1 AS hubspot_company_id
, CAST('2021-12-21' AS DATE) AS contract_start_date
, CAST('2021-12-22' AS DATE) AS contract_end_date
, 39.0 AS monthly_amount

UNION ALL

SELECT
  5 AS subscription_id
, 2 AS hubspot_company_id
, CAST('2021-12-16' AS DATE) AS contract_start_date
, CAST('2021-12-18' AS DATE) AS contract_end_date
, 15.0 AS monthly_amount

UNION ALL

SELECT
  6 AS subscription_id
, 2 AS hubspot_company_id
, CAST('2021-12-18' AS DATE) AS contract_start_date
, CAST('2021-12-20' AS DATE) AS contract_end_date
, 20.0 AS monthly_amount

UNION ALL

SELECT
  7 AS subscription_id
, 2 AS hubspot_company_id
, CAST('2021-12-20' AS DATE) AS contract_start_date
, CAST(NULL AS DATE) AS contract_end_date
, 15.0 AS monthly_amount
