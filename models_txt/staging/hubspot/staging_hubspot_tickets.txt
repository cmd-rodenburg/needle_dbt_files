{{ config(
    tags=["tickets"],
	materialized='table'
) }}



WITH SNAP_TICKETS AS (

	SELECT *
		, ROW_NUMBER() over (partition by "id" order by DBT_VALID_FROM ::TIMESTAMPTZ) AS rn
	FROM BI_SOLYTIC.ANALYTICS_SNAPSHOTS.SNAP_HUBSPOT_TICKETS
	-- ignore deleted tickets - if they are deleted, they won't appear in the source again
	WHERE "id" IN (SELECT "id" FROM {{ source('hubspot', 'tickets') }})

)

, ENGAGEMENTS AS ( -- NOTE: REOPENED/COPIED TICKETS ACQUIRE ALL ENGAGEMENTS FROM PREVIOUS RELATED TICKETS.

	SELECT "id"	::BIGINT 						AS TICKET_ID
			, COUNT(DISTINCT ENGAGEMENT_ID) 	AS NUM_ENGAGEMENTS
	FROM SNAP_TICKETS st
	LEFT JOIN (
		SELECT ticket.value:"id"								::BIGINT 				AS TICKET_ID
			, re."id"											::BIGINT				AS ENGAGEMENT_ID
			, "hs_timestamp"									::TIMESTAMPTZ 			AS CREATED_TIMESTAMP
			, IFF("hs_engagement_type" LIKE '%EMAIL', 'EMAIL',"hs_engagement_type")		AS ENGAGEMENT_TYPE
		FROM {{ source('hubspot_engagements', 'engagements') }} re
			, lateral flatten(input => re."ewah_associations_to_tickets", outer => true) AS ticket
		WHERE ticket.value:"id" IS NOT NULL
	) es ON st."id" = es.TICKET_ID AND st."createdAt"::TIMESTAMPTZ <= es.CREATED_TIMESTAMP
	GROUP BY "id"	::BIGINT

)
, TICKETS_CLEAN AS (
	SELECT
		"id"											::bigint					AS TICKET_ID
		, NULLIF("hs_pipeline"			,'')	 		::text						AS PIPELINE_ID
		, NULLIF("hs_pipeline_stage" 	,'')	 		::integer					AS TICKET_STAGE_ID_OLD
		, NULLIF("hubspot_owner_id"		,'')	 		::bigint					AS OWNER_ID
		-- NOTE: IF TICKET IS LINKED TO SOLYTIC (884813088) FIRST, USE THE SECOND ELEMENT.
		-- NOTE: ALL TICKETS IN KOSTAL PIPELINE ARE LINKED TO KOSTAL (911300494)
		, CASE
			WHEN "ewah_associations_to_companies"[0]:"id" = '884813088' THEN "ewah_associations_to_companies"[1]:"id"
			WHEN "hs_pipeline" = '7279375' THEN 911300494
			ELSE "ewah_associations_to_companies"[0]:"id"
			END 										::bigint					AS COMPANY_ID
		, CASE
			WHEN "ewah_associations_to_companies"[0]:"id" = '884813088' THEN ARRAY_SIZE("ewah_associations_to_companies") -1
			WHEN  "hs_pipeline" = '7279375' THEN 1
			ELSE ARRAY_SIZE("ewah_associations_to_companies")
			END 	::int															AS NUM_COMPANIES
		, "ewah_associations_to_deals"[0]:"id"			::bigint					AS DEAL_ID
		, ARRAY_SIZE("ewah_associations_to_deals")		::int						AS NUM_DEALS
		, "ewah_associations_to_contacts"[0]:"id"		::bigint					AS CONTACT_ID
		, ARRAY_SIZE("ewah_associations_to_contacts")	::int						AS NUM_CONTACTS
		, "subject"																	AS TICKET_NAME
		, NULLIF("createdate"				,'')	 	::TIMESTAMPTZ				AS CREATE_DATE
		, NULLIF("closed_date"				,'')	 	::TIMESTAMPTZ 				AS CLOSED_DATE
		, NULLIF("hs_lastactivitydate"		,'')	 	::date		 				AS LAST_ACTIVITY
		, NULLIF("hs_lastcontacted"			,'')	 	::date		 				AS LAST_CONTACTED
		, NUM_ENGAGEMENTS											 				AS TIMES_CONTACTED
		, NULLIF("first_agent_reply_date"	,'')	 	::TIMESTAMPTZ			 	AS FIRST_REPLY_DATE
		, NULLIF("time_to_first_agent_reply",'')		::bigint					AS TIME_TO_FIRST_CONTACT
		, NULLIF("time_to_close"			,'')	 	::bigint					AS TIME_TO_CLOSE
		, NULLIF("solution_time"			,'')	 	::int						AS TIME_TO_SOLUTION
		, "archived"																AS IS_ARCHIVED
		, IFNULL("fcr" = 'true', FALSE)					::boolean	 				AS FIRST_CONTACT_RESOLUTION
		, NULLIF("assignee"					,'')					 				AS TICKET_ASSIGNEE
		, NULLIF("hs_ticket_priority"		,'')									AS TICKET_PRIORITY
		, UPPER("original_priority"			)					 					AS ORIGINAL_PRIORITY
		, "category"																AS TICKET_CATEGORY
		, "hs_ticket_category"														AS TICKET_CATEGORY_DETAILED
		, "service_katalog"												 			AS SERVICE_KATALOG
		, "portal"																	AS PORTAL
		, "product"																	AS PRODUCT
		, "hs_resolution"															AS RESOLUTION
		, NULLIF("hs_feedback_last_ces_rating"	 ,'')	 ::int		 				AS FEEDBACK_RATING
		, RN
		, DBT_VALID_FROM 								 ::TIMESTAMPTZ				AS VALID_FROM_OLD
		, LEAD(DBT_VALID_FROM ::TIMESTAMPTZ) OVER (PARTITION BY "id" ORDER BY DBT_VALID_FROM ::TIMESTAMPTZ) AS VALID_TO_OLD
	FROM SNAP_TICKETS st
	LEFT JOIN engagements  es ON st."id" = es.TICKET_ID

)

, TICKETS_FIX AS ( -- NOTE: As not always all ticket stages are recorded we need to get the transitions out of the columns saved by hubspot

	 SELECT DISTINCT
		TICKET_ID
		, MAX(STAGE_ENTER)	 AS NEW_VALID_FROM
		, TICKET_STAGE
	FROM (
		SELECT DISTINCT -- NOTE: Extract ticket stages from columns and the time spend in them
			"id"												 ::bigint											AS TICKET_ID
			, SPLIT_PART(TICKET_STAGE, '_', 4)																		AS TICKET_STAGE
			, IFF(SPLIT_PART(TICKET_STAGE, '_', 3) = 'in'		, NEW_VALID_FROM, NULL)								AS TIME_IN_STAGE --needed to identify "skipped" stages
			, IFF(SPLIT_PART(TICKET_STAGE, '_', 3) = 'entered'	, NEW_VALID_FROM, NULL)::TIMESTAMPTZ 				AS STAGE_ENTER
			, RN
		FROM SNAP_TICKETS
		UNPIVOT (NEW_VALID_FROM
			FOR TICKET_STAGE IN (
			"hs_date_entered_1" ,"hs_date_entered_2" ,"hs_date_entered_3" ,"hs_date_entered_4" ,"hs_date_entered_10303609" ,"hs_date_entered_10437311" ,
			"hs_date_entered_14819076" ,"hs_date_entered_15078835" ,"hs_date_entered_7279376" ,"hs_date_entered_7279377" ,"hs_date_entered_7279378" ,
			"hs_date_entered_7279379" ,"hs_date_entered_7572889" ,"hs_date_entered_7572892" ,"hs_date_entered_7576436" ,"hs_date_entered_7576437" ,
			"hs_date_entered_7832703" ,"hs_date_entered_7870381" ,"hs_date_entered_7870382" ,"hs_date_entered_7870383" ,"hs_date_entered_7870384" ,
			"hs_date_entered_8502316" ,"hs_date_entered_8925205" ,"hs_date_entered_8935479" ,"hs_date_entered_9845613" ,"hs_date_entered_9845614" ,
			"hs_date_entered_9845615" ,"hs_date_entered_9860463" ,"hs_date_entered_9860464" ,"hs_date_entered_9860465" ,"hs_date_entered_9860466" ,
			"hs_time_in_1" ,"hs_time_in_2" ,"hs_time_in_3" ,"hs_time_in_4" ,"hs_time_in_10303609" ,"hs_time_in_10437311" ,"hs_time_in_14819076" ,
			"hs_time_in_15078835" ,"hs_time_in_7279376" ,"hs_time_in_7279377" ,"hs_time_in_7279378" ,"hs_time_in_7279379" ,"hs_time_in_7572889" ,"hs_time_in_7572892" ,
			"hs_time_in_7576436" ,"hs_time_in_7576437" ,"hs_time_in_7832703" ,"hs_time_in_7870381" ,"hs_time_in_7870382" ,"hs_time_in_7870383" ,"hs_time_in_7870384" ,
			"hs_time_in_8502316" ,"hs_time_in_8925205" ,"hs_time_in_8935479" ,"hs_time_in_9845613" ,"hs_time_in_9845614" ,"hs_time_in_9845615" ,"hs_time_in_9860463" ,
			"hs_time_in_9860464" ,"hs_time_in_9860465" ,"hs_time_in_9860466"
	 		)
		)
	)
	GROUP BY RN, TICKET_ID, TICKET_STAGE
	HAVING MAX(TIME_IN_STAGE) != 0

)
-- NOTE: MERGE FIXED TICKET STAGES WITH PROPERTIES
, TICKETS_CLEAN_FIX AS(

	SELECT TC.*
			, IFF(TF.TICKET_STAGE IS NULL, TC.TICKET_STAGE_ID_OLD, TICKET_STAGE)							AS TICKET_STAGE_ID
			, IFF(TF.NEW_VALID_FROM IS NULL, TC.VALID_FROM_OLD , TF.NEW_VALID_FROM)	 						AS VALID_FROM
			, IFNULL(LEAD(VALID_FROM) OVER (PARTITION BY TC.TICKET_ID ORDER BY VALID_FROM),'9999-12-31') 	AS VALID_TO
	FROM TICKETS_CLEAN TC
	-- NOTE: SORT THE STAGES TO THE DIFFERENT PROPERTIY ROWS BY BETWEEN()
	LEFT JOIN TICKETS_FIX TF ON TF.TICKET_ID = TC.TICKET_ID
		AND (
			(TF.NEW_VALID_FROM >= TC.VALID_FROM_OLD AND TF.NEW_VALID_FROM < TC.VALID_TO_OLD)
			OR (TF.NEW_VALID_FROM <= TC.VALID_FROM_OLD AND RN = 1))

)
-- NOTE: TICKETS CAN LOOP THROUGH THE SAME STAGES MULTIPLE TIMES. THEREFORE THE ISLAND/GAP METHOD IS APPLIED TO INDICATE THE ISLANDS
, TICKET_ISLANDS AS (

	SELECT *
		, SUM (IFF(PREVIOUS_STAGE = HASH_,0,1)) OVER (PARTITION BY TICKET_ID ORDER BY RN_) 			AS ISLAND_ID
	FROM (
		-- NOTE: IDENTIFY THE PREVIOUS STAGE ID
		SELECT *
			, HASH(TICKET_ID, PIPELINE_ID, TICKET_STAGE_ID_OLD, OWNER_ID, COMPANY_ID, NUM_COMPANIES, DEAL_ID, NUM_DEALS, CONTACT_ID, NUM_CONTACTS, TICKET_NAME, IS_ARCHIVED, FIRST_CONTACT_RESOLUTION, TICKET_ASSIGNEE, TICKET_PRIORITY, ORIGINAL_PRIORITY, TICKET_CATEGORY, TICKET_CATEGORY_DETAILED, SERVICE_KATALOG, PORTAL, PRODUCT, RESOLUTION, FEEDBACK_RATING, RN, TICKET_STAGE_ID) AS HASH_
			, LAG(TICKET_STAGE_ID) OVER (PARTITION BY TICKET_ID ORDER BY VALID_TO, VALID_FROM  ) 	AS PREVIOUS_STAGE
			, ROW_NUMBER() OVER (PARTITION BY TICKET_ID ORDER BY VALID_FROM ) 						AS RN_
		FROM TICKETS_CLEAN_FIX
	)
)

SELECT DISTINCT
	TICKET_ID
	, PIPELINE_ID
	, TICKET_STAGE_ID
	, OWNER_ID
	, COMPANY_ID
	, NUM_COMPANIES
	, DEAL_ID
	, NUM_DEALS
	, CONTACT_ID
	, NUM_CONTACTS
	, TICKET_NAME
	, MIN(CREATE_DATE)			OVER (PARTITION BY TICKET_ID, ISLAND_ID)		 		AS CREATE_DATE
	, MAX(CLOSED_DATE)			OVER (PARTITION BY TICKET_ID, ISLAND_ID)		 		AS CLOSED_DATE
	, MAX(LAST_ACTIVITY)		OVER (PARTITION BY TICKET_ID, ISLAND_ID)				AS LAST_ACTIVITY
	, MAX(LAST_CONTACTED)		OVER (PARTITION BY TICKET_ID, ISLAND_ID)		 		AS LAST_CONTACTED
	, MAX(TIMES_CONTACTED)		OVER (PARTITION BY TICKET_ID, ISLAND_ID)		 		AS TIMES_CONTACTED
	, MAX(FIRST_REPLY_DATE)		OVER (PARTITION BY TICKET_ID, ISLAND_ID)		 		AS FIRST_REPLY_DATE
	, MAX(TIME_TO_FIRST_CONTACT)OVER (PARTITION BY TICKET_ID, ISLAND_ID)/3600000.0 		AS TIME_TO_FIRST_CONTACT
	, MAX(TIME_TO_CLOSE)		OVER (PARTITION BY TICKET_ID, ISLAND_ID)/3600000.0		AS TIME_TO_CLOSE
	, MAX(TIME_TO_SOLUTION)		OVER (PARTITION BY TICKET_ID, ISLAND_ID)				AS TIME_TO_SOLUTION
	, IS_ARCHIVED
	, FIRST_CONTACT_RESOLUTION
	, TICKET_ASSIGNEE
	, TICKET_PRIORITY
	, ORIGINAL_PRIORITY
	, TICKET_CATEGORY
	, SERVICE_KATALOG
	, PORTAL
	, PRODUCT
	, RESOLUTION
	, FEEDBACK_RATING
	, MIN(VALID_FROM)	OVER (PARTITION BY TICKET_ID, HASH_, ISLAND_ID) 				AS VALID_FROM
	, MAX(VALID_TO)		OVER (PARTITION BY TICKET_ID, HASH_, ISLAND_ID) 				AS VALID_TO
	, MAX(VALID_TO)		OVER (PARTITION BY TICKET_ID, HASH_, ISLAND_ID)  = '9999-12-31'	AS ACTIVE_INDICATOR
	, ISLAND_ID
FROM TICKET_ISLANDS