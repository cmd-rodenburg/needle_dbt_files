{{ config(
    tags=["engagements"],
	materialized='table'
) }}



WITH base_engage as (

	SELECT re."id"											::BIGINT					AS ENGAGEMENT_ID
		, deal.value:"id"									::BIGINT 					AS DEAL_ID
		, company.value:"id"								::BIGINT					AS COMPANY_ID
		, "ewah_associations_to_contacts"[0]:"id"			::BIGINT					AS CONTACT_ID
		, "ewah_associations_to_tickets"[0]:"id"			::BIGINT					AS TICKET_ID
		, nullif("hubspot_owner_id"	, '')					::BIGINT 					AS OWNER_ID
		, nullif("hubspot_team_id"	, '')					::BIGINT 					AS TEAM_ID
		, "hs_timestamp"									::TIMESTAMP WITH TIME ZONE	AS TIMESTAMP_KEY
		, IFNULL(ARRAY_SIZE ("ewah_associations_to_contacts"),0) 						AS NUM_CONTACTS
		, IFNULL(ARRAY_SIZE ("ewah_associations_to_companies"),0) 						AS NUM_COMPANIES
		, IFNULL(ARRAY_SIZE ("ewah_associations_to_deals"	),0) 						AS NUM_DEALS
		, IFNULL(ARRAY_SIZE ("ewah_associations_to_tickets"	),0) 						AS NUM_TICKETS
		, IFF("hs_engagement_type" LIKE '%EMAIL', 'EMAIL',"hs_engagement_type")			AS ENGAGEMENT_TYPE
		, CASE
			WHEN "hs_engagement_type" = 'EMAIL' 										THEN 'Outgoing email'
			WHEN "hs_engagement_type" LIKE '%EMAIL' 									THEN REPLACE(UPPER("hs_engagement_type"), '_', ' ')
			WHEN "hs_engagement_type" = 'TASK' 											THEN "hs_task_status"
			WHEN "hs_engagement_type" = 'NOTE' 											THEN 'NOTE'
			WHEN  "hs_engagement_type" ='LINKEDIN_MESSAGE'								THEN 'LinkedIn message'
			WHEN "hs_meeting_outcome" IN ('RESCHEDULED', 'NO_SHOW')						THEN 'TERMINATED'
			WHEN  "hs_engagement_type" = 'MEETING'										THEN 'COMPLETED'
			WHEN "hs_call_disposition" = 'edc3777f-5aaa-4036-a608-3e6aa49d0402'			THEN '0-5 min'
			WHEN "hs_call_disposition" = '3f8cdbaa-5e53-40e1-a25f-f782a5aa923c'			THEN '5-15 min'
			WHEN "hs_call_disposition" = '04ee2269-793d-4c34-8b9c-132946ed5f63'			THEN '15-30 min'
			WHEN "hs_call_disposition" = 'c7e0257e-dbe0-4603-b25c-4e0677c33ac1'			THEN '>30 min'
			WHEN "hs_call_disposition" IN ('73a0d17f-1163-4015-bdd5-ec830791da20', '9d9162e7-6cf3-4944-bf63-4dff82258764', 'a4c4c377-d246-4b32-a13b-75a56a4cd0ff', 'b2cf5968-551e-4856-9783-52b3da59a7d0')
				OR("hs_engagement_type" = 'CALL' AND "hs_call_duration" IS NULL)		THEN 'No answer'
			WHEN "hs_call_disposition" = '17b47fee-58de-441e-a44c-c6300d46f273'			THEN 'Wrong number'
			WHEN "hs_call_disposition" = 'f240bbac-87c9-4f6e-bf70-924b57d47db7'
				OR ("hs_engagement_type" = 'CALL' AND "hs_call_duration" IS not NULL) THEN 'COMPLETED'
		end 																				AS PRE_OUTCOME
		, CASE
			WHEN PRE_OUTCOME IN ('0-5 min' ,'5-15 min', '15-30 min', 'COMPLETED', '>30 min')  		THEN 'COMPLETED'
			WHEN PRE_OUTCOME IN ('No answer', 'Unsuccessful' ) OR PRE_OUTCOME LIKE 'NOT_STARTED%'	THEN 'TERMINATED'
			ELSE PRE_OUTCOME
		END 																			AS  OUTCOME
		, round(CASE
			WHEN "hs_engagement_type" = 'MEETING' 			THEN DATEDIFF(MINUTE, "hs_meeting_start_time"::timestamp, "hs_meeting_end_time"::timestamp)
			WHEN "hs_engagement_type" = 'CALL'				THEN (NULLIF("hs_call_duration", '')::int/1000) /60
			WHEN "hs_engagement_type" LIKE '%EMAIL' 		THEN 30
			WHEN "hs_engagement_type" ='LINKEDIN_MESSAGE'	THEN 30
		end ,0)																				AS ENGAGEMENT_DURATION
		, CASE
			WHEN OUTCOME IN ('TERMINATED', 'Wrong number')									THEN '0-5 min'
			WHEN (engagement_duration between 0 and 5) OR OUTCOME = 'Outgoing email'		THEN '0-5 min'
			WHEN engagement_duration between 5 and 15 										THEN '5-15 min'
			WHEN ENGAGEMENT_DURATION between 15 and 60 OR OUTCOME = '>30 min' 				THEN '15-60 min'
			WHEN ENGAGEMENT_DURATION between 60 and 120 									THEN '60-120 min'
			WHEN ENGAGEMENT_DURATION > 120													THEN '> 120 min'
			WHEN OUTCOME LIKE 'COMPLETED'	OR 	OUTCOME = 'NOTE'							THEN 'Unknown duration'
			ELSE OUTCOME
		end 																				AS 	DURATION_CATEGORY
	, CASE
		WHEN ENGAGEMENT_DURATION IS NULL AND OUTCOME IN ('NOT_STARTED', 'FORWARDED_EMAIL','INCOMING_EMAIL') 	THEN 0
		WHEN OUTCOME IN ('No answer', 'NOTE', 'TERMINATED') 			THEN 3
		WHEN ENGAGEMENT_DURATION IS NULL AND OUTCOME = '0-5 min'		THEN 5
		WHEN ENGAGEMENT_DURATION IS NULL AND OUTCOME = '5-15 min'		THEN 15
		WHEN ENGAGEMENT_DURATION IS NULL AND OUTCOME = '15-30 min'		THEN 30
		WHEN ENGAGEMENT_DURATION IS NULL AND OUTCOME = '>30 min'		THEN 60
		WHEN ENGAGEMENT_DURATION IS NULL AND OUTCOME = 'COMPLETED'		THEN 15
		ELSE ENGAGEMENT_DURATION
		END 																				AS  ACTUAL_DURATION_MIN
		, round(CASE
			WHEN ENGAGEMENT_TYPE = 'MEETING' 	THEN ACTUAL_DURATION_MIN + 30
			WHEN ENGAGEMENT_TYPE = 'CALL'		THEN ACTUAL_DURATION_MIN + 15
			ELSE ENGAGEMENT_DURATION
		end ,0)															AS TOTAL_DURATION_MIN
	FROM {{ source('hubspot_engagements', 'engagements') }} re
		, lateral flatten(input => re."ewah_associations_to_deals", outer => true) AS deal
		, lateral flatten(input => re."ewah_associations_to_companies", outer => true) AS company
	WHERE re."archived" = false
		-- NOTE: EXCLUDE SOLYTIC COMPANY IDS
		AND (company.value:"id" NOT IN ('884813088', '8115393283', '3057537083')
			OR company.value:"id" IS NULL )

)
, deal_to_company AS (

	SELECT DISTINCT DEAL_ID
		, COMPANY_ID
		, UPPER(DEAL_NAME) LIKE '%@SOLYTIC.COM%' AS solytic_indicator
	FROM  {{ ref('staging_hubspot_deals') }}
	WHERE ACTIVE_INDICATOR = TRUE
		AND ( COMPANY_ID IS NOT NULL OR
			UPPER(DEAL_NAME) LIKE '%@SOLYTIC.COM%')

	)


SELECT
	ENGAGEMENT_ID
	, DEAL_ID
	, IFNULL(she.COMPANY_ID, dc.COMPANY_ID) 									AS COMPANY_ID
	, CONTACT_ID
	, TICKET_ID
	, OWNER_ID
	, TEAM_ID
	, TIMESTAMP_KEY
	, COUNT(DISTINCT DEAL_ID) OVER (PARTITION BY ENGAGEMENT_ID) 				AS NUM_DEALS
	, COUNT(DISTINCT COMPANY_ID) OVER (PARTITION BY ENGAGEMENT_ID) 				AS NUM_COMPANIES
	, NUM_CONTACTS
	, NUM_TICKETS
	, ENGAGEMENT_TYPE
	, OUTCOME
	, DURATION_CATEGORY
	, ACTUAL_DURATION_MIN
	, TOTAL_DURATION_MIN
FROM base_engage she
LEFT JOIN deal_to_company dc USING (DEAL_ID)
WHERE (NUM_DEALS > 0 OR NUM_COMPANIES > 0)
	-- NOTE: AS ALL THE ASSOCIATIONS ARE PIVOTED RANDOMLY TO EVERY DEAL COMBINATION, COMBINE THE DEALS TO THE CORRECT COMPANY
	-- NOTE: EXCLUDE SOLYTIC FAKE DEALS
	AND ((she.COMPANY_ID IS NOT NULL AND she.COMPANY_ID = dc.COMPANY_ID)
		OR dc.DEAL_ID IS NULL
		OR she.COMPANY_ID IS NULL)
	AND ((dc.DEAL_ID IS NOT NULL AND SOLYTIC_INDICATOR = FALSE) OR dc.DEAL_ID IS NULL)