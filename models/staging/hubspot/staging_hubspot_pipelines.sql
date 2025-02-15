{{ config(
    tags=["deals"],
	materialized='table'
) }}

WITH STAGES_CLEAN AS (

	SELECT
		PL."object_type"															AS OBJECT_TYPE
		, PL."id" 																	AS PIPELINE_ID
		, PL."label" 																AS PIPELINE_NAME
		, ST.VALUE:"id"::string														AS STAGE_ID
		, LOWER(ST.VALUE:"label"::string) 											AS OG_LOWER_STAGE_NAME
		, ST.VALUE:"metadata":"probability"	 ::FLOAT = 1 							AS IS_WON
		-- NOTE: GROUP DEAL STAGES IN CAMELCASE
		, INITCAP(IFF(OBJECT_TYPE = 'deals', CASE
				WHEN IS_WON = TRUE 									THEN 'Won'
				WHEN OG_LOWER_STAGE_NAME LIKE '%archiv%' 			THEN 'Lost'
				WHEN OG_LOWER_STAGE_NAME LIKE '%mql%'
					OR OG_LOWER_STAGE_NAME LIKE '%lukewarm%' 		THEN 'Marketing Qualified Lead'
				WHEN OG_LOWER_STAGE_NAME LIKE '%sql%'
					OR OG_LOWER_STAGE_NAME LIKE '%warm%' 			THEN 'Sales Qualified Lead'
				WHEN OG_LOWER_STAGE_NAME LIKE '%product seen%'
					OR OG_LOWER_STAGE_NAME LIKE '%hot%'				THEN 'Dealmaking'
				WHEN OG_LOWER_STAGE_NAME LIKE '%offer expired%' 	THEN 'Offer Sent'
				WHEN OG_LOWER_STAGE_NAME LIKE 'contract terminated' THEN 'Contract Ended'
				WHEN OG_LOWER_STAGE_NAME LIKE '%cold%'
					OR OG_LOWER_STAGE_NAME LIKE '%backlog%'
					OR OG_LOWER_STAGE_NAME LIKE '%lost%'			THEN 'Lost'
				ELSE OG_LOWER_STAGE_NAME
				END,
			OG_LOWER_STAGE_NAME)) 													AS STAGE_NAME
		, CASE STAGE_NAME
			when 'New Lead'					THEN 0
			when 'Marketing Qualified Lead'	THEN 1
			when 'Sales Qualified Lead'		THEN 2
			when 'Dealmaking'				THEN 3
			when 'Offer Sent'				THEN 4
			when 'Won'						THEN 5
			when 'Contract Ended'			THEN 6
			when 'Lost'						THEN 7
			ELSE ST.VALUE:"displayOrder" ::INT
		END																			AS STAGE_DISPLAY_ORDER
		,ST.VALUE:"metadata":"isClosed"::BOOLEAN 									AS IS_CLOSED
		, COALESCE(DBT_VALID_TO, '9999-12-31'::TIMESTAMP)							AS VALID_TO
	, MAX(VALID_TO) OVER (PARTITION BY STAGE_ID) = VALID_TO							AS ACTIVE
	FROM BI_SOLYTIC.ANALYTICS_SNAPSHOTS.SNAP_HUBSPOT_PIPELINES AS PL
		-- Stage information are documented within each pipeline
		, LATERAL FLATTEN(INPUT => PL."stages") AS ST

)

SELECT
	DENSE_RANK () OVER (ORDER BY OBJECT_TYPE, STAGE_NAME) AS PIPELINE_SK
	, OBJECT_TYPE
	, PIPELINE_ID
	, PIPELINE_NAME
	, STAGE_ID
	, STAGE_NAME
	, MAX(STAGE_DISPLAY_ORDER) OVER (PARTITION BY OBJECT_TYPE, STAGE_NAME) AS STAGE_DISPLAY_ORDER
	, IS_WON
	, IFF(STAGE_NAME = 'Lost', TRUE, IS_CLOSED)									AS IS_CLOSED
FROM STAGES_CLEAN
WHERE ACTIVE = TRUE