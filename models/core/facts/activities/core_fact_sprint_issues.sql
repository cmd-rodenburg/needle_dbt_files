
WITH story_points_time AS (

	-- NOTE: CALCULATE AVERAGE TIME SPENT BY STORYPOINTS AND TEAM
	SELECT TEAM
		, STORY_POINTS
		, round(AVG(TIME_SPENT) ,1)	 AS DUMMY_TIME
	FROM {{ ref('staging_flatfiles_jira_issues') }} a
	WHERE TIME_SPENT IS NOT NULL
		AND STORY_POINTS IS NOT NULL
	GROUP BY STORY_POINTS
		, TEAM

)

SELECT sfji.ISSUE_ID
	, sfji.ISSUE_KEY
	, sfji.RESOLUTION_DATE								AS DATE_KEY
	, sfji.STORY_POINTS
	-- NOTE: IF TIME SPENT IS MISSING, ADD DUMMY TIME
	, ROUND(IFNULL(sfji.TIME_SPENT, spt.DUMMY_TIME),1)	AS TIME_HOURS
	, sfji.ISSUE_TYPE
	, sfji.TEAM
	, sfji.NUMBER_SUBTASKS
	, cdc.COMPANY_SK
	, cdo.OWNER_SK
	, sfji.ASSIGNEE
FROM {{ ref('staging_flatfiles_jira_issues') }}	sfji
LEFT JOIN story_points_time spt ON sfji.TEAM = spt.TEAM
	AND sfji.STORY_POINTS = spt.STORY_POINTS
	AND sfji.TIME_SPENT IS NULL
LEFT JOIN {{ ref('core_dim_company') }} cdc USING (COMPANY_ID)
FULL JOIN {{ ref('core_dim_owner') }}   cdo ON sfji.ASSIGNEE = cdo.EMPLOYEE_NAME
	AND sfji.RESOLUTION_DATE >= CDO.VALID_FROM
	AND sfji.RESOLUTION_DATE < CDO.VALID_TO
WHERE ISSUE_STATUS IN ('Done', 'Closed')
	AND ISSUE_TYPE NOT IN ('Sub-task', 'Epic')


