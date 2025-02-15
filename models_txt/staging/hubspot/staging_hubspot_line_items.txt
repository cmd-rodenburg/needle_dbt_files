WITH line_items AS (

	select *
	from "BI_SOLYTIC"."ANALYTICS_SNAPSHOTS".snap_hubspot_line_items


), line_deals as (

	select distinct
		b_d."id"				::BIGINT 							AS DEAL_ID
		, l_i.value:"id"		::BIGINT							AS LINE_ITEM_ID
		, b_d."ewah_associations_to_companies"[0]:"id" ::BIGINT		AS COMPANY_ID
	FROM {{ source('hubspot', 'deals') }} as b_d
		, lateral flatten(input => b_d."ewah_associations_to_line_items") AS l_i

)

select
	 ld.company_id													AS COMPANY_ID
	, ld.deal_id													AS DEAL_ID
	, ld.line_item_id												AS LINE_ITEM_ID
	, li."hs_product_id"			::BIGINT 						AS PRODUCT_ID
	, li."hs_created_by_user_id"									AS USER_ID_CREATED
	, li."hs_updated_by_user_id"									AS USER_ID_UPDATED
	, li."name" 													AS LINE_ITEM_NAME
	, CASE
    	WHEN PRODUCT_ID IN (1042576758,1061785154,111522984)    THEN 'volume-based setup'
    	WHEN PRODUCT_ID IN (1061785157, 1042576486)             THEN 'single-site setup'
    	WHEN PRODUCT_ID IN (1042576490,1061785155)              THEN 'single-site saas'
    	WHEN PRODUCT_ID IN (1061881371,1042576759,111569732)    THEN 'volume-based saas'
    	WHEN PRODUCT_ID = 1061785150                            THEN 'WL saas'
    	WHEN PRODUCT_ID = 1061785151                            THEN 'WL setup'
    	WHEN PRODUCT_ID IN (1133151893, 1133151887)             THEN 'service level agreement'
    	WHEN PRODUCT_ID IN (1320150434,1320152750)              THEN 'product & services'
    	WHEN PRODUCT_ID IN (1441323542,1435373934)              THEN 'API'
    	WHEN PRODUCT_ID IN (1061785142,1061785147)              THEN 'contractual work'
    	WHEN PRODUCT_ID IN (1063766950, 1063766952)             THEN 'discount'
    	ELSE 'other'    END 		                            	AS LINE_ITEM_CATEGORY
	, li."description"												AS DESCRIPTION
	, li."archived"								::BOOLEAN			AS ARCHIVED
	, NULLIF(li."quantity"				, '')	::INT				AS QUANTITY_UNITS
	, NULLIF(li."price"					, '')	::NUMERIC(10,2)	 	AS UNIT_PRICE
	, NULLIF(li."hs_pre_discount_amount", '')	::NUMERIC(10,2)		AS PRE_DISCOUNT_AMOUNT
	, NULLIF(li."discount"				, '')	::NUMERIC(10,2)		AS DISCOUNT
	, NULLIF(li."hs_total_discount"		, '')	::NUMERIC(10,2)	 	AS TOTAL_DISCOUNT
	, NULLIF(li."hs_discount_percentage", '')	::NUMERIC(10,2)	 	AS DISCOUNT_PERCENTAGE
	, NULLIF(li."amount"				, '')	::NUMERIC(10,2)		AS AMOUNT
	, li."recurringbillingfrequency"								AS RECURRINGBILLINGFREQUENCY
	, li."hs_recurring_billing_start_date"							AS RECURRING_BILLING_START_DATE
	, li."hs_recurring_billing_end_date"							AS RECURRING_BILLING_END_DATE
	, li."hs_recurring_billing_period"								AS RECURRING_BILLING_PERIOD
	, NULLIF(li."hs_term_in_months"		, '')		::INT			AS TERM_IN_MONTHS
	, NULLIF(li."hs_acv"				, '')		::NUMERIC(10,2) AS ACV
	, NULLIF(li."hs_arr"				, '')		::NUMERIC(10,2) AS ARR
	, NULLIF(li."hs_cost_of_goods_sold" , '')		::NUMERIC 		AS COST_OF_GOODS_SOLD
	, NULLIF(li."hs_margin"				, '')		::NUMERIC(10,2) AS MARGIN
	, NULLIF(li."hs_margin_acv"			, '')	 	::NUMERIC(10,2) AS MARGIN_ACV
	, NULLIF(li."hs_margin_arr"			, '')		::NUMERIC(10,2) AS MARGIN_ARR
	, NULLIF(li."hs_margin_mrr"			, '')		::NUMERIC(10,2) AS MARGIN_MRR
	, NULLIF(li."hs_margin_tcv"			, '')		::NUMERIC(10,2) AS MARGIN_TCV
	, NULLIF(li."hs_mrr"				, '')		::NUMERIC(10,2) AS MRR
	, li."hs_position_on_quote"						::INT			AS POSITION_ON_QUOTE
	, li."hs_tcv"									::NUMERIC(10,2)	AS TCV
	, IFNULL(li.DBT_VALID_FROM, '2001-01-01') 		::DATE			AS VALID_FROM
	, IFNULL(li.DBT_VALID_TO,  	'9999-12-31') 		::DATE			AS VALID_TO
	, li.DBT_VALID_TO IS NULL 										AS ACTIVE_INDICATOR
-- NOTE: THE RELATIONSHIP BETWEEN DEALS AND LINE ITEMS IS LOCATED IN A JSONB COLUMN
FROM line_items	AS li
LEFT JOIN line_deals ld ON ld.LINE_ITEM_ID = cast(li."id" AS BIGINT)
WHERE LD.LINE_ITEM_ID IS NOT NULL