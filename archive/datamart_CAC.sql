{{ config(
    tags=["engagements"]
) }}

-- COST per HOUR per Employee
WITH WAGE_PER_HOUR AS (

	SELECT DISTINCT
		cdo."hubspot_owner_id"		AS "owner_id"
		, cdo."owner_sk"			AS "owner_sk"
		, cdo."full_name"
		, "gross_amount"
		, CASE
			WHEN "employment_type" = 'Working student' THEN 13
			WHEN "employment_type" = 'Internship'      THEN 13
			ELSE ROUND("gross_amount" / (cdo."weekly_working_hour"*4), 2)
		END														AS COST_PER_HOUR
		, cdo."employment_type"
		, cfe."receipt_date"
		, ROW_NUMBER () OVER (PARTITION BY cdo."hubspot_owner_id" ORDER BY cfe."receipt_date" DESC) AS rn_
	FROM {{ ref("core_fact_invoiced_expenses") }} cfe
	LEFT JOIN {{ ref("core_dim_owner") }} cdo ON REPLACE(cfe."line_item_description", 'SC - ', '') = cdo."full_name"
	WHERE cfe."line_item_description" LIKE 'SC - %'
		AND "accounting_type" = 681772
		AND cdo."hubspot_owner_id" IS NOT NULL
	

	)

-- TIME SPENT PER DEAL
, ENGAGEMENTS AS (

	SELECT "company_id"
		, "owner_sk"
		, IFF(SUM("duration_mins") = 0, 0, SUM("duration_mins")/60)		AS SUM_ENGAGEMENT_HOUR
		, COUNT(*)														AS NUM_ENGAGEMENTS
		, IFF(WON_DATE IS NULL, FALSE, TRUE)							AS ACQUIRED_COMPANY
	FROM (
			SELECT  DISTINCT
			COALESCE(cdd."company_id" ,cdc."company_id") AS "company_id"
			, cfe."owner_sk"
			, cfe."duration_mins"
			, cfe."engagement_id"
			, cfe."created_timestamp"
			, IFNULL(MIN(IFF(cdd."poc_indicator" = TRUE, NULL, cdd."won_date")) OVER (PARTITION BY COALESCE(cdd."company_id" ,cdc."company_id")), NULL) AS WON_DATE
		FROM {{ ref("core_fact_engagements") }} cfe
		LEFT JOIN {{ ref("core_dim_deal") }} cdd USING ("deal_sk")
		LEFT JOIN {{ ref("core_dim_company") }} cdc USING ("company_sk")
	)
	WHERE WON_DATE <= "created_timestamp" OR WON_DATE IS NULL
	GROUP BY "company_id"
		, "owner_sk"
		, ACQUIRED_COMPANY


)

SELECT IFNULL(wph."owner_sk", 0)									AS OWNER_SK
	, e."company_id"
	, e.NUM_ENGAGEMENTS
	, e.sum_engagement_hour							AS TOTAL_HOURS
	, e.sum_engagement_hour * wph.COST_PER_HOUR 	AS TOTAL_COSTS
	, e.ACQUIRED_COMPANY
FROM ENGAGEMENTS		e
LEFT JOIN WAGE_PER_HOUR wph USING ("owner_sk")
WHERE rn_ = 1
	OR rn_ IS NULL

