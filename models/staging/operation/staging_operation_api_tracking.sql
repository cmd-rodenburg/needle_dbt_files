{{
  config(
    materialized='incremental'
    )
}}
-- NOTE: The original table contains currently 880 million rows. To reduce the duration of the run, incremental insert has been implemented


SELECT sa.EVENT_DATE						AS DATE_KEY
  , mu.CUSTOMERID 							AS OPERATIONAL_ID 
  , sa.USER_ID                  AS OPERATIONAL_USER_ID
  , COUNT(*)										AS COUNT_QUERIES
FROM {{ source('operation_tracking', 'api_tracking') }} sa
LEFT JOIN  {{ source('operation', 'user') }} mu ON mu.ID = sa.USER_ID

{% if is_incremental() %}

  -- NOTE: this filter will only be applied on an incremental run
  WHERE sa.EVENT_DATE > (select max(DATE_KEY) from {{ this }})

{% endif %}

GROUP BY sa.EVENT_DATE
  , sa.USER_ID
  , mu.CUSTOMERID

