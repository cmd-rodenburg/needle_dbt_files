WITH ATTENDANCE AS (
	SELECT "employee"                                           ::BIGINT    AS EMPLOYEE_ID
 		, "date"												::DATE	 	AS DATE_KEY
		, "start_time"											::TIME		AS START_TIME
		, IFF("end_time" = '24:00','23:59:59.999',"end_time" )	::TIME		AS END_TIME
		, "break"															AS BREAK_MINUTES
	FROM {{source('personio', 'attendances') }}
	WHERE "status" = 'confirmed'
)

SELECT EMPLOYEE_ID
    , DATE_KEY
    , SUM((TIMEDIFF(MINUTE, START_TIME, END_TIME)-BREAK_MINUTES)/60 ::NUMERIC(8,2)) 	AS WORKING_HOURS
FROM ATTENDANCE
GROUP BY EMPLOYEE_ID
    , DATE_KEY