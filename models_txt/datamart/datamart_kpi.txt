{{ config(
    tags=["deals"]
) }}

WITH Relevant_stages AS (	-- NOTE: SELECT RELEVANT DEALS AND APPLY HISTORIZED STAGE NAMES

	SELECT DEAL_SK
		, DEAL_STAGE_NAME
		, VALID_FROM
		, VALID_TO
		, ANNUAL_CONTRACT_VALUE
		, LAG(DEAL_STAGE_NAME) OVER (PARTITION BY DEAL_SK ORDER BY VALID_FROM, VALID_TO)		AS PREVIOUS_STAGE
	, ROW_NUMBER () OVER (PARTITION BY DEAL_SK ORDER BY VALID_FROM, VALID_TO) 					AS RN_
	FROM {{ ref('core_dim_deal') }} SHD

)
, DEAL_ISLANDS AS ( -- NOTE: DEALS CAN LOOP THROUGH THE SAME STAGES MULTIPLE TIMES. THEREFORE THE ISLAND/GAP METHOD IS APPLIED TO INDICATE THE ISLANDS

	SELECT DEAL_SK
		, DEAL_STAGE_NAME
		, VALID_FROM
		, VALID_TO
		, SUM(IFF(PREVIOUS_STAGE = DEAL_STAGE_NAME, 0, 1)) OVER (PARTITION BY DEAL_SK ORDER BY RN_) 			AS ISLAND_ID
		, LAST_VALUE(ANNUAL_CONTRACT_VALUE) OVER (PARTITION BY DEAL_SK, DEAL_STAGE_NAME ORDER BY VALID_FROM) 	AS ANNUAL_CONTRACT_VALUE
	FROM Relevant_stages

)

SELECT DATE_KEY
	, REPLACE(KPI_NAME, ' in ', '')		AS KPI_CATEGORY
	, CONCAT(KPI_NAME, DEAL_STAGE) 		AS KPI_NAME
	, KPI_VALUE
FROM (
	SELECT DATE_KEY
		, DEAL_STAGE
		, COUNT(DISTINCT DEAL_SK)		::INT	AS "Deals in "
		, SUM(ANNUAL_CONTRACT_VALUE)	::INT	AS "Revenue in "
		, ROUND(AVG(TIME_IN_STAGE),4)	::INT	AS "Day duration in "
		FROM (
			SELECT
				dd.WEEK_FIRST_DAY 																AS DATE_KEY
				, CFD.DEAL_STAGE_NAME 															AS DEAL_STAGE
				, IFNULL(cfd.ANNUAL_CONTRACT_VALUE, 0 )											AS ANNUAL_CONTRACT_VALUE
				, cfd.DEAL_SK
				, DATEDIFF(DAY, cfd.VALID_FROM,
					LEAD(cfd.VALID_FROM) OVER (PARTITION BY cfd.DEAL_SK ORDER BY VALID_FROM)) 	AS TIME_IN_STAGE
				FROM (
					SELECT
						DEAL_SK
						, DEAL_STAGE_NAME
						, ANNUAL_CONTRACT_VALUE
						, MIN(VALID_FROM) 		AS VALID_FROM
						, ISLAND_ID
					FROM DEAL_ISLANDS
					GROUP BY
						DEAL_SK
						, DEAL_STAGE_NAME
						, ANNUAL_CONTRACT_VALUE
						, ISLAND_ID
						 ) cfd
		LEFT JOIN {{ ref('core_dim_date') }} dd ON cfd.VALID_FROM ::DATE = DD.DATE_KEY
			WHERE cfd.DEAL_STAGE_NAME IN ('Marketing Qualified Lead','Sales Qualified Lead', 'Dealmaking', 'Offer Sent', 'Won', 'Contract Ended','Lost')
			) part_1
	GROUP BY DEAL_STAGE
		, DATE_KEY
	) part_2
unpivot(KPI_VALUE FOR KPI_NAME IN ("Deals in ", "Revenue in ", "Day duration in "))

UNION All

SELECT DATE_KEY
	, SPLIT_PART(KPI_NAME, ' ', 1)		AS KPI_CATEGORY
	, KPI_NAME
	, KPI_VALUE
FROM (
	SELECT
		cdd.WEEK_FIRST_DAY 																	AS DATE_KEY
		, COUNT(DISTINCT IFF(ena.WAS_WON = TRUE, ena.ENGAGEMENT_ID, NULL))			::INT	AS "Engagements by week All - Won"
		, COUNT(DISTINCT ena.ENGAGEMENT_ID)											::INT	AS "Engagements by week All"
		, COUNT(DISTINCT IFF(ena.EARLY_STAGES = TRUE, ena.ENGAGEMENT_ID, NULL))		::INT	AS "Engagements by week SQL"
		, "Engagements by week All" - "Engagements by week SQL" 					::INT	AS "Engagements by week MQL"
		, COUNT(DISTINCT IFF(ENGAGEMENT_TYPE = 'MEETING', ena.ENGAGEMENT_ID, NULL))	::INT	AS "Meetings per week"
	 	, COUNT(DISTINCT IFF(ENGAGEMENT_TYPE = 'CALL', ena.ENGAGEMENT_ID, NULL))	::INT	AS "Calls per week"
	FROM (
		-- NOTE: SELECT RELEVANT ENGAGEMENTS AND EXCLUDE TASKS AND NOTES
		SELECT DISTINCT
			cfe.ENGAGEMENT_ID
			, DATE(cfe.TIMESTAMP_KEY)																				AS DATE_KEY
			, cfe.DEAL_STAGE_NAME																					AS ENGAGEMENT_DEAL_STAGE
			, CFE.ENGAGEMENT_TYPE
			, CDD.DEAL_STAGE_NAME IN ('Won', 'Contract Ended') 								AS WAS_WON
			, cfe.DEAL_STAGE_NAME IN ('Offer Sent', 'Dealmaking', 'Sales Qualified Lead') 		AS EARLY_STAGES
		FROM {{ ref('core_fact_engagements') }} cfe
		LEFT JOIN {{ ref('core_dim_deal') }} CDD ON CDD.DEAL_SK = CFE.DEAL_SK AND CDD.ACTIVE_INDICATOR = TRUE
		WHERE cfe.ENGAGEMENT_TYPE NOT IN ('TASK', 'NOTE')
			AND cfe.DEAL_STAGE_NAME IN ('Offer Sent', 'Dealmaking', 'Sales Qualified Lead', 'Lost', 'Marketing Qualified Lead')
		) ena
	LEFT JOIN {{ ref('core_dim_date') }} cdd USING (DATE_KEY)
	GROUP BY cdd.WEEK_FIRST_DAY
	HAVING "Engagements by week All" != 0
	)
unpivot( KPI_VALUE FOR KPI_NAME IN (
	 "Engagements by week All - Won"
	, "Engagements by week All"
	, "Engagements by week SQL"
	, "Engagements by week MQL"
	, "Meetings per week"
	, "Calls per week" ))