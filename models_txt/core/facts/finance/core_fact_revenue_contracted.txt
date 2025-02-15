WITH LINE_ITEMS AS (

    SELECT
        DEAL_ID
        , LINE_ITEM_ID
        , PRODUCT_ID
        , LINE_ITEM_NAME
        , LINE_ITEM_CATEGORY
        , RECURRING_BILLING_START_DATE
        , RECURRING_BILLING_END_DATE
        , IFF(LINE_ITEM_CATEGORY IN ('volume-based saas','volume-based setup'),
            QUANTITY_UNITS/1000,QUANTITY_UNITS)                             AS QUANTITY_UNITS
        , IFF(LINE_ITEM_CATEGORY IN ('volume-based saas','volume-based setup'),
            UNIT_PRICE * 1000,UNIT_PRICE)                                   AS UNIT_PRICE
        , TOTAL_DISCOUNT
        , AMOUNT
    	, ARR
        , MRR
        , VALID_FROM
        , VALID_TO
    FROM {{ ref('staging_hubspot_line_items') }}
    GROUP BY
        DEAL_ID
        , LINE_ITEM_ID
        , PRODUCT_ID
        , LINE_ITEM_NAME
        , LINE_ITEM_CATEGORY
        , RECURRING_BILLING_START_DATE
        , RECURRING_BILLING_END_DATE
        , QUANTITY_UNITS
        , UNIT_PRICE
        , TOTAL_DISCOUNT
        , AMOUNT
    	, ARR
        , MRR
    	, VALID_FROM
        , VALID_TO

)

, DEALS AS (

    SELECT
        DEAL_ID
        , DEAL_STAGE_SK
        , DEAL_SK
        , COMPANY_SK
        , CONTRACT_START_DATE
        , CONTRACT_END_DATE
        , PAYMENT_SCHEMA
        , VALID_FROM
        , VALID_TO
    FROM {{ ref('core_dim_deal') }}
    WHERE VALID_TO > CONTRACT_START_DATE
    	AND VALID_FROM < CONTRACT_END_DATE
    	AND DEAL_STAGE_NAME IN ('Won')

)

, MERGES as(

    SELECT
        de.DEAL_SK
        , li.DEAL_ID
        , de.DEAL_STAGE_SK
        , de.COMPANY_SK
        , li.LINE_ITEM_ID
        , li.PRODUCT_ID
        , li.LINE_ITEM_NAME
        , li.LINE_ITEM_CATEGORY
        , li.RECURRING_BILLING_START_DATE
        , li.RECURRING_BILLING_END_DATE
        , li.QUANTITY_UNITS
        , li.UNIT_PRICE
        , li.TOTAL_DISCOUNT
        , li.AMOUNT
    	, li.ARR
        , li.MRR
        , GREATEST(li.VALID_FROM, de.VALID_FROM)                                                    AS VALID_FROM_NEW
        , LEAST(li.VALID_TO, de.VALID_TO)                                                           AS VALID_TO_NEW
        , CASE
        	WHEN VALID_FROM_NEW < li.RECURRING_BILLING_START_DATE    THEN li.RECURRING_BILLING_START_DATE
        	WHEN VALID_FROM_NEW < de.CONTRACT_START_DATE             THEN de.CONTRACT_START_DATE
        	ELSE VALID_FROM_NEW                                   END               ::DATE			AS VALID_FROM_M
        , CASE
        	WHEN VALID_TO_NEW > li.RECURRING_BILLING_END_DATE        THEN li.RECURRING_BILLING_END_DATE
        	WHEN VALID_TO_NEW > de.CONTRACT_END_DATE                 THEN de.CONTRACT_END_DATE
        	ELSE VALID_TO_NEW                                     END               ::DATE 			AS VALID_TO_M
    FROM LINE_ITEMS li
    Inner JOIN DEALS de ON de.DEAL_ID = li.DEAL_ID
       AND li.VALID_FROM < de.VALID_TO
       AND li.VALID_TO > de.VALID_FROM
    WHERE VALID_TO_M > CONTRACT_START_DATE
        AND VALID_FROM_M < CONTRACT_END_DATE
)

SELECT
    DEAL_SK
    , DEAL_STAGE_SK
    , DEAL_ID
    , COMPANY_SK
    , LINE_ITEM_ID
    , PRODUCT_ID
    , LINE_ITEM_NAME
    , LINE_ITEM_CATEGORY
    , RECURRING_BILLING_START_DATE
    , RECURRING_BILLING_END_DATE
    , QUANTITY_UNITS
    , UNIT_PRICE
    , TOTAL_DISCOUNT
    , AMOUNT
    , ARR
    , MRR
    , MIN(VALID_FROM_M)                                                                 AS VALID_FROM
    -- NOTE: AS dim deal is broken right now, this fills the gaps
    -- NOTE: When fixed replace with "MAX(VALID_TO) AS VALID_TO"
    , LEAD(MIN(VALID_FROM_M), 1,MAX(VALID_TO_M)) OVER (
           PARTITION BY DEAL_SK, LINE_ITEM_ID
           ORDER BY MIN(VALID_FROM_M))                                                  AS VALID_TO
    , IFF(CURRENT_DATE >= VALID_FROM AND CURRENT_DATE < VALID_TO, TRUE, FALSE)          AS ACTIVE_INDICATOR
    , LAG(QUANTITY_UNITS) OVER (PARTITION BY DEAL_SK, LINE_ITEM_ID ORDER BY VALID_FROM) AS LAG_QUANTITY_UNITS
    , IFF(QUANTITY_UNITS = LAG_QUANTITY_UNITS, FALSE, TRUE)                             AS CHANGE_INDICATOR
FROM MERGES
WHERE (RECURRING_BILLING_START_DATE < VALID_TO_M
        OR RECURRING_BILLING_START_DATE IS NULL)
GROUP BY DEAL_SK
    , DEAL_STAGE_SK
    , DEAL_ID
    , COMPANY_SK
    , LINE_ITEM_ID
    , PRODUCT_ID
    , LINE_ITEM_NAME
    , LINE_ITEM_CATEGORY
    , RECURRING_BILLING_START_DATE
    , RECURRING_BILLING_END_DATE
    , QUANTITY_UNITS
    , UNIT_PRICE
    , TOTAL_DISCOUNT
    , AMOUNT
    , ARR
    , MRR
