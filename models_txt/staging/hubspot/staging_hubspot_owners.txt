{{ config(
    tags=["gdpr"]
) }}


SELECT
	"id"				::BIGINT		AS OWNER_ID
	, "email"							AS EMAIL
	, "firstName" 						AS FIRST_NAME
	, "lastName" 						AS LAST_NAME
	, "firstName" || ' ' || "lastName" 	AS EMPLOYEE_NAME
	, "userId"						 	AS USER_ID
	, "teams"[0]:"name"	::STRING		AS TEAM_1
FROM BI_SOLYTIC.ANALYTICS_SNAPSHOTS.SNAP_HUBSPOT_OWNERS
WHERE DBT_VALID_TO IS NULL
	AND EMPLOYEE_NAME IS NOT NULL
