WITH ABSENCE AS (

	SELECT
		 "employee"		::BIGINT										AS EMPLOYEE_ID
		, "status"			::VARCHAR(32)								AS APPROVAL_STATUS
		, "time_off_type":"attributes":"category" ::VARCHAR(32)			AS ORIGINAL_ABSENCE_TYPE
		, IFF(ORIGINAL_ABSENCE_TYPE = 'offsite_work', NULL, ORIGINAL_ABSENCE_TYPE) AS ABSENCE_TYPE
		, IFF(ORIGINAL_ABSENCE_TYPE = 'offsite_work', TRUE, FALSE)		AS HOMEOFFICE
		, "start_date"		::DATE 										AS START_DATE
		, "end_date"		::DATE 										AS END_DATE
		, "half_day_start"	::BOOLEAN									AS HALF_DAY_START
		, "half_day_end"	::BOOLEAN									AS HALF_DAY_END
	FROM {{source('personio', 'absences') }}
	WHERE APPROVAL_STATUS = 'approved'
)

SELECT
	cdd."DATE"																				AS DATE_KEY
	, EMPLOYEE_ID																			AS EMPLOYEE_ID
	, IFF(LISTAGG(ABSENCE_TYPE, ';') = '', NULL, LISTAGG(DISTINCT ABSENCE_TYPE, ';'))		AS ABSENCE_TYPE
	, SUM(CASE
		WHEN "DATE" = START_DATE AND HALF_DAY_START = TRUE AND ABSENCE_TYPE IS NOT NULL THEN 0.5
		WHEN "DATE" = END_DATE AND HALF_DAY_END = TRUE AND ABSENCE_TYPE IS NOT NULL  	THEN 0.5
		WHEN ABSENCE_TYPE IS NOT NULL													THEN 1
		END) 																				AS ABSENCE_DAYS
	, SUM(CASE
	WHEN "DATE" = START_DATE AND HALF_DAY_START = TRUE AND HOMEOFFICE = TRUE  	THEN 0.5
	WHEN "DATE" = END_DATE AND HALF_DAY_END = TRUE AND HOMEOFFICE = TRUE 		THEN 0.5
	WHEN HOMEOFFICE = TRUE THEN 1
	END) 																					AS HOMEOFFICE_DAYS
FROM ABSENCE
LEFT JOIN {{ ref('gemma_dates') }} cdd ON cdd."DATE" BETWEEN START_DATE AND END_DATE
GROUP BY
	CDD."DATE",
	EMPLOYEE_ID