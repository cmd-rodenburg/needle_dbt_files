SELECT PARSE_JSON(PARENT):id 							::INT			AS ISSUE_ID
	, PARSE_JSON(PARENT):key 							::VARCHAR 		AS ISSUE_KEY
	, PARSE_JSON(PARENT):summary 						::VARCHAR 		AS ISSUE_NAME
	, IFF(PARSE_JSON(PROJECT) IS NOT NULL
		,PARSE_JSON(PROJECT):"name", NULL) 				::VARCHAR		AS PROJECT_NAME
	, IFF(ASSIGNEE = 'None', NULL,
		CASE PARSE_JSON(ASSIGNEE):"displayName"			::VARCHAR
		WHEN 'Mariam Tevzadze' 		THEN 'Mariami Tevzadze'
		WHEN 'Siddharth D''Souza' 	THEN 'Sid D''Souza'
		WHEN 'lukasz.goncerzewicz' 	THEN 'Lukasz Goncerzewicz'
		WHEN 'Axel Leßmeister' 		THEN 'Axel Lessmeister'
		WHEN 'Fabian Lueghausen' 	THEN 'Fabian Lüghausen'
		WHEN 'JustinaND' 			THEN 'Justina Nainyte-Dogdu'
		ELSE PARSE_JSON(ASSIGNEE):"displayName"
		END 										::VARCHAR)			AS ASSIGNEE
	, IFF(REPORTER = 'None', NULL,
		CASE  PARSE_JSON(REPORTER):"displayName" 		::VARCHAR
			WHEN 'Mariam Tevzadze' 		THEN 'Mariami Tevzadze'
			WHEN 'Siddharth D''Souza' 	THEN 'Sid D''Souza'
			WHEN 'lukasz.goncerzewicz' 	THEN 'Lukasz Goncerzewicz'
			WHEN 'Axel Leßmeister' 		THEN 'Axel Lessmeister'
			WHEN 'Fabian Lueghausen' 	THEN 'Fabian Lüghausen'
			WHEN 'JustinaND' 			THEN 'Justina Nainyte-Dogdu'
			ELSE  PARSE_JSON(REPORTER):"displayName" 	::VARCHAR
		END)															AS REPORTER
	, PARSE_JSON(status):"name"							::VARCHAR		AS ISSUE_STATUS
	, PARSE_JSON(issuetype):"name" 						::VARCHAR		AS ISSUE_TYPE
	, PARSE_JSON("fixVersions")[0]:"name"::VARCHAR						AS COMPANY_NAME
	, CASE
		WHEN COMPANY_NAME LIKE '%STEAG%'	                   	THEN 2884954891
	    when COMPANY_NAME LIKE '%PV Maint%'                   	THEN 6216722298
	    when COMPANY_NAME LIKE '%KOSTAL%'                     	THEN 911300494
	    when COMPANY_NAME LIKE '%Greentech%'                  	THEN 5278560320
	    when COMPANY_NAME LIKE '%MBG%'         					THEN 911616294
	    when COMPANY_NAME LIKE '%AT, Martin Hirschhofer GmbH%'	THEN 8721650518
	END 	         									::INT          	AS COMPANY_ID
	, LEFT(created,10)									::DATE			AS CREATE_DATE
	, customfield_10115 								::VARIANT 		AS SPRINTS
	, TRY_CAST(customfield_10119 AS INT) 								AS STORY_POINTS
	, TRY_CAST(customfield_10215 AS DATE)								AS EPIC_START_DATE
	, TRY_CAST(duedate AS DATE)											AS DUE_DATE
	, IFF(ISSUE_TYPE NOT IN ('Sub-task', 'Epic'), ISSUE_NAME, NULL) 	AS EPIC_NAME
	, PARSE_JSON(TIMETRACKING):"timeSpentSeconds"/3600 	::INT			AS TIME_SPENT
	, PARSE_JSON(priority):"name" 						::VARCHAR		AS PRIORITY
	, ARRAY_SIZE(PARSE_JSON(SUBTASKS))				::INT				AS NUMBER_SUBTASKS
	, IFF(customfield_10112 = 'None',
		NULL,
		PARSE_JSON(customfield_10112):"name" ::VARCHAR) 				AS TEAM
	, TRY_CAST(LEFT(resolutiondate, 10) AS DATE) 						AS RESOLUTION_DATE
FROM {{ source('flatfiles', 'jira_issues') }}