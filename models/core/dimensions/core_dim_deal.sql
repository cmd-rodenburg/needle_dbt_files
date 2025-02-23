{{ config(
    tags=["deals"]
) }}


-- STEPS
---- 1. Current record
---- 2. Changing stages
---- 3. Line items
---- 4. First win
---- 5. TOTAL join

WITH DEALS_RELEVANT AS (

	-- NOTE: SELECT DEALS CURRENTLY IN RELEVANT PIPELINES
	SELECT SHD.*
		, IFF(SHP.IS_WON = TRUE, MIN(SHD.VALID_FROM) OVER (PARTITION BY SHD.DEAL_ID), NULL) AS FIRST_WIN
	FROM {{ ref('staging_hubspot_deals') }} SHD
	LEFT JOIN {{ ref('staging_hubspot_pipelines') }} SHP ON SHD.DEAL_STAGE_ID = SHP.STAGE_ID
	WHERE SHD.ACTIVE_INDICATOR = TRUE
		AND SHD.PIPELINE_ID in ('6505895', 'default')

)
, DEAL_STAGES AS (

	-- NOTE: SELECT INTERESTING COLUMNS FOR SLOWLY CHANGING DIMENSIONS TYPE 2 (ONLY DEAL STAGE FOR NOW)
	SELECT SHD.DEAL_ID
		, DENSE_RANK ()	OVER (ORDER BY SHD.DEAL_ID)								AS DEAL_SK
		, ROW_NUMBER() OVER (ORDER BY SHD.DEAL_ID, MIN(SHD.VALID_FROM))				AS DEAL_STAGE_SK
		, SHD.DEAL_STAGE_ID
		, SHP.STAGE_NAME 		AS DEAL_STAGE_NAME
		, SHP.IS_WON
		, SHP.IS_CLOSED
		, MIN(SHD.VALID_FROM) 													AS VALID_FROM
		, MAX(SHD.VALID_TO) 													AS VALID_TO
		, IFF(MAX(SHD.VALID_TO) = '9999-12-31', TRUE, FALSE)					AS ACTIVE_INDICATOR
	FROM (
		SELECT *
			, SUM(CASE WHEN PREVIOUS_HASH = HASH_ THEN 0 ELSE 1 END) OVER (PARTITION BY DEAL_ID ORDER BY RN_) AS ISLAND_ID
		FROM (
			SELECT DEAL_ID
				, DEAL_STAGE_ID
				, VALID_FROM
				, VALID_TO
				, HASH(DEAL_ID, DEAL_STAGE_ID) 													AS HASH_
				, LAG(HASH_) OVER (PARTITION BY SHD.DEAL_ID ORDER BY VALID_FROM, VALID_TO) 		AS PREVIOUS_HASH
				, ROW_NUMBER() OVER (PARTITION BY SHD.DEAL_ID ORDER BY VALID_FROM, VALID_TO) 	AS RN_
			FROM {{ ref('staging_hubspot_deals') }} SHD
			WHERE VALID_FROM <>  VALID_TO
			) island
		) SHD
	LEFT JOIN {{ ref('staging_hubspot_pipelines') }} SHP ON SHD.DEAL_STAGE_ID = SHP.STAGE_ID
		AND OBJECT_TYPE = 'deals'
	GROUP BY SHD.DEAL_ID
		, SHD.DEAL_STAGE_ID
		, SHP.STAGE_NAME
		, SHD.ISLAND_ID
		, SHP.IS_WON
		, SHP.IS_CLOSED

)

, LINE_ITEMS AS (

	SELECT li.DEAL_ID
		, SUM(IFF((li.LINE_ITEM_CATEGORY = 'volume-based saas' AND NOT de.POC_INDICATOR)
			OR (li.LINE_ITEM_CATEGORY = 'volume-based setup' AND de.POC_INDICATOR)
			, li.QUANTITY_UNITS, NULL)/1000 ) 													AS VOLUME_BASED_MWP
		, SUM(IFF((li.LINE_ITEM_CATEGORY = 'single-site saas' AND NOT de.POC_INDICATOR)
			OR (li.LINE_ITEM_CATEGORY = 'single-site setup' AND de.POC_INDICATOR)
			, li.QUANTITY_UNITS, NULL) ) 					 									AS SINGLE_SITES
		, SUM(li.ACV)																			AS ANNUAL_CONTRACT_VALUE
		, SUM(li.ARR)																			AS ANNUAL_RECURRING_REVENUE
		, SUM(li.MRR)																			AS MONTHLY_RECURRING_REVENUE
		, SUM(IFF(li.RECURRINGBILLINGFREQUENCY IS NULL, ACV,0))								AS ONE_OFF_VALUE
		, SUM(li.TCV)																			AS TOTAL_CONTRACT_VALUE
	FROM  {{ ref('staging_hubspot_line_items') }} li
	LEFT JOIN {{ ref('staging_hubspot_deals') }} de ON li.DEAL_ID = de.DEAL_ID
		AND de.ACTIVE_INDICATOR = TRUE
	WHERE li.VALID_TO = '9999-12-31'
	GROUP BY li.DEAL_ID
)

SELECT
	t2.DEAL_SK
	, t1.DEAL_ID
	, t2.DEAL_STAGE_SK
	, CDC.COMPANY_SK
	, t2.DEAL_STAGE_NAME
	, t2.IS_WON
	, t2.IS_CLOSED
	, t1.DEAL_NAME
	, IFNULL(CDO.OWNER_SK, -1)										AS OWNER_SK
	, t1.CUSTOMER_SEGMENT
	, t1.DEAL_PERSONA
	, t1.DEAL_TYPE
	, t1.DEAL_CATEGORY
	, t1.POC_INDICATOR
	, t1.WHITELABEL_INDICATOR
	, t1.LOST_TYPE
	, t1.OFFER_EXPIRATION_DATE
	, t1.DAYS_SINCE_LAST_CONTACT
	, t1.EXPECTED_DEAL_CLOSING_MONTH
	, t1.DAYS_TO_CLOSE
	, t1.CREATE_DATE
	, t1.CLOSE_DATE
	, t1.FIRST_WIN													AS WON_DATE
	, t1.CONTRACT_START_DATE
	, t1.CONTRACT_END_DATE
	, t1.CONTRACT_DURATION
	, t1.AUTOMATIC_RENEWAL
	, t1.AUTOMATIC_RENEWAL_PERIOD
	, t1.CONTRACT_NOTICE_PERIOD
	, t1.CONTRACT_TERMINATED_TYPE
	, SHLI.VOLUME_BASED_MWP
	, SHLI.SINGLE_SITES
	-- NOTE: ANNUAL CONTRACT VALUE == AMOUNT, BUT ANNUAL CONTRACT VALUE IS BASED ON LINE ITEMS, WHILE AMOUNT IS MANUALLY FILLED
	, IFNULL(SHLI.ANNUAL_CONTRACT_VALUE, t1.AMOUNT)					AS ANNUAL_CONTRACT_VALUE
	, SHLI.ANNUAL_RECURRING_REVENUE 								AS ANNUAL_RECURRING_REVENUE
	, SHLI.MONTHLY_RECURRING_REVENUE 								AS MONTHLY_RECURRING_REVENUE
	, SHLI.ONE_OFF_VALUE 											AS ONE_OFF_VALUE
	, SHLI.TOTAL_CONTRACT_VALUE 									AS TOTAL_CONTRACT_VALUE
	, t1.PAYMENT_TYPE
	, t1.PAYMENT_PERIOD
	, CASE t1.PAYMENT_PERIOD
		WHEN 'yearly' 		THEN 12
		WHEN 'quarterly' 	THEN 3
		WHEN 'monthly' 		THEN 1
		WHEN 'biyearly' 	THEN 6
		ELSE 0
	END																AS PAYMENT_OCCURRENCE
	, t1.PAYMENT_SCHEMA
	, t1.LAGGING
	, t1.BILLING_EMAIL
	, t1.IBAN
	, t1.READY_FOR_ACCOUNTING
	, t1.ACCOUNT_HOLDER
	, t1.ACCOUNTING_ISSUES
	, t1.BANT_BUDGET
	, t1.BANT_AUTHORITY
	, t1.BANT_NEED
	, t1.BANT_TIMING
	, t2.VALID_FROM 		:: DATE 								AS VALID_FROM
	, t2.VALID_TO			:: DATE 								AS VALID_TO
	, t2.ACTIVE_INDICATOR
FROM DEALS_RELEVANT t1
LEFT JOIN DEAL_STAGES t2 USING (DEAL_ID)
LEFT JOIN LINE_ITEMS SHLI USING (DEAL_ID)
LEFT JOIN {{ ref('core_dim_company') }} CDC USING (COMPANY_ID)
LEFT JOIN {{ ref('core_dim_owner') }} CDO USING (OWNER_ID)
WHERE CDO.ACTIVE_INDICATOR = TRUE

UNION ALL

SELECT -1					AS DEAL_SK
	, -1					AS DEAL_ID
	, -1					AS DEAL_STAGE_SK
	, NULL					AS COMPANY_SK
	, NULL
	, NULL
	, NULL
	, 'UNKNOWN'				AS DEAL_NAME
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, NULL
	, '2017-12-31' ::DATE		AS VALID_FROM
	, '9999-12-31' ::DATE		AS VALID_TO
	, TRUE						AS ACTIVE_INDICATOR
