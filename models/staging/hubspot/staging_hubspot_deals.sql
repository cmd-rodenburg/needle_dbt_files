{{ config(
    tags=["deals"],
	materialized='table'
) }}


WITH SNAP_DEALS AS (

	SELECT *
	FROM "BI_SOLYTIC"."ANALYTICS_SNAPSHOTS".snap_hubspot_deals shd
		-- NOTE: IGNORE DELETED DEALS - IF THEY ARE DELETED, THEY WON'T APPEAR IN THE SOURCE AGAIN
	WHERE "id" IN (SELECT "id" FROM {{ source('hubspot', 'deals') }})

)
, DEAL_CLEAN as (

	SELECT
		"id" 												::BIGINT 								AS DEAL_ID
		-- NOTE: TO PREVENT CONFUSION USE THE FIRST COMPANY FOR ALL VALUES
		, LAST_VALUE("ewah_associations_to_companies"[0]:"id" ::BIGINT)
			OVER (PARTITION BY "id" ORDER BY DBT_VALID_FROM ::TIMESTAMP) 							AS COMPANY_ID
		, NULLIF("hubspot_owner_id", '')					::BIGINT 								AS OWNER_ID
		, "dealstage"																				AS DEAL_STAGE_ID
		, "pipeline"																				AS PIPELINE_ID
		, "dealname"																				AS DEAL_NAME
		, "customer_segment"																		AS CUSTOMER_SEGMENT
		, "deal_persona"																			AS DEAL_PERSONA
		, CASE "dealtype"
			WHEN 'newbusiness' 		THEN 'New Business'
			WHEN 'existingbusiness' THEN 'Existing Business'
		END																							AS DEAL_TYPE
		, IFF("b2b_deal_kategorie" = 'PoC / Test', 'POC', "b2b_deal_kategorie")						AS DEAL_CATEGORY
		, IFF("test_deal" = 'true' OR "b2b_deal_kategorie" = 'PoC / Test', TRUE, FALSE) 			AS POC_INDICATOR
		, IFNULL("whitelabel_basis" = 'true', FALSE) 												AS WHITELABEL_INDICATOR
		, CASE "lost_reason"
			WHEN 'not our customer'       THEN 'Disqualified Lead'
			WHEN 'Signed with competitor' THEN 'Product'
			WHEN 'Will use free portals'  THEN 'Price'
			WHEN 'Not reachable'          THEN 'Timing'
		END																							AS LOST_TYPE
		, "offer_expiration_date"																	AS OFFER_EXPIRATION_DATE
		, NULLIF("expected_deal_closing_month", '')			::DATE 									AS EXPECTED_DEAL_CLOSING_MONTH
		, NULLIF("days_to_close", '')			::NUMERIC(8,2)										AS DAYS_TO_CLOSE
		, "createdAt" 							::DATE												AS CREATE_DATE
		, NULLIF("closedate",'')				::DATE												AS CLOSE_DATE
		, NULLIF("contract_start_date", '')		::DATE												AS CONTRACT_START_DATE
		, NULLIF("contract_end_date", '')		::DATE												AS CONTRACT_END_DATE
		, (CONTRACT_END_DATE - CONTRACT_START_DATE) ::NUMERIC(8,2)  								AS CONTRACT_DURATION
		, IFF("automatic_renewal" = 'true', TRUE, FALSE)											AS AUTOMATIC_RENEWAL
		, NULLIF("automatic_renewal_period"	, '')			::NUMERIC(8,2)							AS AUTOMATIC_RENEWAL_PERIOD
		, NULLIF("contract_notice_period"	, '')			::NUMERIC(8,2)							AS CONTRACT_NOTICE_PERIOD
		, "contract_terminated_reason"																AS CONTRACT_TERMINATED_TYPE
		, DATEDIFF(DAY, NULLIF("notes_last_contacted", '')	::DATE, CURRENT_DATE())					AS DAYS_SINCE_LAST_CONTACT
		, "payment_type"																			AS PAYMENT_TYPE
		, IFF("paymant_period" != '', "paymant_period", NULL)										AS PAYMENT_PERIOD
		, NULLIF("payment_schema", '') 																AS PAYMENT_SCHEMA
		, IFF("billing_mode" = 'Lagging', TRUE, FALSE)												AS LAGGING
		, NULLIF("billing_email", '')																AS BILLING_EMAIL
		, IFF(LENGTH("iban") > 15, UPPER(REPLACE("iban", ' ', '')), NULL)							AS IBAN
		, NULLIF("account_holder", '')																AS ACCOUNT_HOLDER
		, IFF("ready_for_accounting" = 'true', TRUE,FALSE)											AS READY_FOR_ACCOUNTING
		, IFF("issues" = 'true', TRUE, FALSE)														AS ACCOUNTING_ISSUES
		 {% for bant in ['"budget_of_lead_assessment"', '"bant__authority_rating"' , '"bant__need_rating"', '"bant__timing_rating"' ] %}
		, CASE {{ bant }}
			WHEN 'low' 		THEN -1
			WHEN 'medium' 	THEN 0
			WHEN 'high' 	THEN 1
		end																						AS {{ bant|upper }}
			{%- endfor -%}
		, NULLIF("amount", '')		::NUMERIC(10,2)												AS AMOUNT
		, NULLIF("hs_arr", '')		::NUMERIC(10,2) 											AS ANNUAL_RECURRING_REVENUE
		, DBT_VALID_FROM ::TIMESTAMP															AS VALID_FROM
		, IFNULL(DBT_VALID_TO, '9999-12-31')	::TIMESTAMP										AS VALID_TO
		, ROW_NUMBER() OVER (PARTITION BY "id" ORDER BY DBT_VALID_FROM ::TIMESTAMP) 			AS RN
	FROM SNAP_DEALS
)
, DEAL_ISLANDS AS ( -- NOTE: DEALS CAN LOOP THROUGH THE SAME STAGES MULTIPLE TIMES. THEREFORE THE ISLAND/GAP METHOD IS APPLIED TO INDICATE THE ISLANDS

	SELECT *
		, SUM (CASE WHEN PREVIOUS_STAGE = HASH_ THEN 0 ELSE 1 END) OVER (PARTITION BY DEAL_ID ORDER BY RN_) AS ISLAND_ID
	FROM (
		SELECT *
			, HASH(DEAL_STAGE_ID, DEAL_ID, OWNER_ID, PIPELINE_ID, DEAL_NAME, CUSTOMER_SEGMENT, DEAL_PERSONA, DEAL_TYPE, DEAL_CATEGORY, POC_INDICATOR, WHITELABEL_INDICATOR, LOST_TYPE, OFFER_EXPIRATION_DATE, EXPECTED_DEAL_CLOSING_MONTH, DAYS_TO_CLOSE, CREATE_DATE, CLOSE_DATE, CONTRACT_START_DATE, CONTRACT_END_DATE, CONTRACT_DURATION, AUTOMATIC_RENEWAL, AUTOMATIC_RENEWAL_PERIOD, CONTRACT_NOTICE_PERIOD, CONTRACT_TERMINATED_TYPE, PAYMENT_TYPE, PAYMENT_PERIOD, PAYMENT_SCHEMA, LAGGING, BILLING_EMAIL, IBAN, ACCOUNT_HOLDER, READY_FOR_ACCOUNTING, ACCOUNTING_ISSUES, AMOUNT, ANNUAL_RECURRING_REVENUE, BUDGET_OF_LEAD_ASSESSMENT, BANT__AUTHORITY_RATING, BANT__NEED_RATING, BANT__TIMING_RATING) AS HASH_
			, LAG(HASH_) 	OVER (PARTITION BY DEAL_ID ORDER BY VALID_FROM, VALID_TO) 		AS PREVIOUS_STAGE
			, LAG(OWNER_ID) OVER (PARTITION BY DEAL_ID ORDER BY VALID_FROM, VALID_TO) 		AS PREVIOUS_OWNER
			, ROW_NUMBER() 	OVER (PARTITION BY DEAL_ID  ORDER BY  VALID_FROM, VALID_TO) 	AS RN_
		FROM DEAL_CLEAN
 	) A
)

SELECT DISTINCT
	DEAL_ID
	, OWNER_ID
	, COMPANY_ID
	, PIPELINE_ID
	, DEAL_STAGE_ID
	, DEAL_NAME
	, CUSTOMER_SEGMENT
	, DEAL_PERSONA
	, DEAL_TYPE
	, DEAL_CATEGORY
	, POC_INDICATOR
	, WHITELABEL_INDICATOR
	, LOST_TYPE
	, OFFER_EXPIRATION_DATE
	, MIN(DAYS_SINCE_LAST_CONTACT)	OVER (PARTITION BY DEAL_ID, HASH_, ISLAND_ID) 			AS DAYS_SINCE_LAST_CONTACT
	, EXPECTED_DEAL_CLOSING_MONTH
	, DAYS_TO_CLOSE
	, CREATE_DATE
	, CLOSE_DATE
	, CONTRACT_START_DATE
	, CONTRACT_END_DATE
	, CONTRACT_DURATION
	, AUTOMATIC_RENEWAL
	, AUTOMATIC_RENEWAL_PERIOD
	, CONTRACT_NOTICE_PERIOD
	, CONTRACT_TERMINATED_TYPE
	, PAYMENT_TYPE
	, PAYMENT_PERIOD
	, PAYMENT_SCHEMA
	, LAGGING
	, BILLING_EMAIL
	, IBAN
	, ACCOUNT_HOLDER
	, READY_FOR_ACCOUNTING
	, ACCOUNTING_ISSUES
	, BUDGET_OF_LEAD_ASSESSMENT			AS BANT_BUDGET
    , BANT__AUTHORITY_RATING			AS BANT_AUTHORITY
    , BANT__NEED_RATING					AS BANT_NEED
    , BANT__TIMING_RATING				AS BANT_TIMING
	, AMOUNT
	, ANNUAL_RECURRING_REVENUE
	, MIN(VALID_FROM)	OVER (PARTITION BY DEAL_ID, HASH_, ISLAND_ID) 					AS VALID_FROM
	, MAX(VALID_TO)	OVER (PARTITION BY DEAL_ID, HASH_, ISLAND_ID) 						AS VALID_TO
	, MAX(VALID_TO)	OVER (PARTITION BY DEAL_ID, HASH_, ISLAND_ID)  = '9999-12-31'		AS ACTIVE_INDICATOR
FROM DEAL_ISLANDS