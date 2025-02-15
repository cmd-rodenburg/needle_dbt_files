WITH BASIC_SNAP AS (

	SELECT
		"id" ::BIGINT															AS EMPLOYEE_ID
		, CASE CONCAT("first_name", ' ', "last_name")
			WHEN 'Axel Leßmeister' 					THEN 'Axel Lessmeister'
			WHEN 'Constantin von Armansperg'		THEN 'Constantin Armansperg'
			WHEN 'Christoph Roland Petri'			THEN 'Christoph Petri'
			WHEN 'Kimberley Sampson-Collett' 		THEN 'Kimberley Sampson'
			WHEN 'Konrad Perényi' 					THEN 'Konrad Perenyi'
			WHEN 'Nikolaus Nather' 					THEN 'Niko Nather'
			WHEN 'Phillip Bekim Lehmann' 			THEN 'Phillip Lehmann'
			WHEN 'Siddharth D''Souza' 				THEN 'Sid D''Souza'
			WHEN 'Tra Nguyen' 						THEN 'Thi Bich Tra Nguyen'
			WHEN 'Yassmine Rahoui' 					THEN 'Jassmine Rahoui'
			ELSE CONCAT("first_name", ' ',	"last_name")
			END 																AS EMPLOYEE_NAME
		, CONCAT("last_name", ' ',"first_name")									AS EMPLOYEE_NAME_REVERSE
		, INITCAP( CASE "gender"
			WHEN 'Männlich' THEN 'male'
			WHEN 'Weiblich' THEN 'female'
			ELSE "gender"	END		)				::VARCHAR(32)   			AS GENDER
		, CASE "department":"attributes":"name"
			WHEN 'Working students / Interns' 	THEN "team":"attributes":"name"
			WHEN 'Product Management ' 			THEN 'Product Management'
			ELSE "department":"attributes":"name" END ::VARCHAR(32)   			AS SUB_DEPARTMENT
		, CASE
			WHEN SUB_DEPARTMENT in ('Tech',
									'Analytics',
									'Product Management',
									'Marketplace') 			THEN 'Product & Tech'
			WHEN SUB_DEPARTMENT IN ('Customer Success',
									'Sales',
									'Marketing') 			THEN 'Sales'
			WHEN SUB_DEPARTMENT = 'Operations' 				THEN 'Business operations'
			ELSE 'Other' 				END 		::VARCHAR(32)				AS DEPARTMENT
		, IFF("employment_type" = 'external' OR
			"employment_type" = 'Extern',TRUE, FALSE)							AS EXTERNAL_EMPLOYEE
		, CASE "dynamic_181223"
			WHEN 'Apprentice' THEN 'Internship'
			ELSE "dynamic_181223" END				::VARCHAR(100)   			AS EMPLOYMENT_TYPE
		, TRY_CAST(REPLACE("weekly_working_hours", '.00','') AS INT)			AS WEEKLY_WORKING_HOUR
		, IFF("status" = 'active' OR "status" = 'Aktiv', TRUE, FALSE)			AS ACTIVE_EMPLOYEE
		, "hire_date"								::DATE						AS EMPLOYMENT_START_DATE
		, IFF(
			LAST_VALUE("contract_end_date"::DATE) OVER (PARTITION BY "id" ORDER BY DBT_VALID_FROM) = "contract_end_date"::DATE,
			"contract_end_date"::DATE, NULL)									AS CONTRACT_END_DATE
		, IFF(LAST_VALUE("termination_date"::DATE) OVER (PARTITION BY "id" ORDER BY DBT_VALID_FROM) = "termination_date"::DATE,
			"termination_date"::DATE, NULL)										AS TERMINATION_DATE
		, "termination_type"						::VARCHAR(120)				AS TERMINATION_REASON
		, DBT_VALID_FROM 														AS VALID_FROM
		, IFNULL(DBT_VALID_TO, '9999-12-31')									AS VALID_TO
		-- NOTE: Hash the slowly changing dimension values for comparison
		, HASH(CONCAT(IFNULL(DEPARTMENT,''),
				IFNULL(SUB_DEPARTMENT,''),
				IFNULL(EMPLOYMENT_TYPE,''),
				IFNULL( WEEKLY_WORKING_HOUR,0))) 						AS HASH_key
	FROM BI_SOLYTIC.ANALYTICS_SNAPSHOTS.SNAP_PERSONIO_EMPLOYEES
	-- NOTE: Exclude failed deletion
	WHERE EMPLOYEE_ID != 14687818

), ISLANDS AS (

	SELECT *
		, SUM (CASE WHEN PREVIOUS_HASH = HASH_key  THEN 0 ELSE 1 END) OVER (PARTITION BY EMPLOYEE_ID ORDER BY RN_) AS ISLAND_ID
	FROM (
		SELECT *
			, LAG(HASH_key) OVER (PARTITION BY EMPLOYEE_ID ORDER BY VALID_FROM, VALID_TO) AS PREVIOUS_HASH
			, ROW_NUMBER() OVER (PARTITION BY EMPLOYEE_ID  ORDER BY  VALID_FROM, VALID_TO) AS RN_
		FROM BASIC_SNAP
	) island_part_1

 ), MIN_MAX AS (

	-- NOTE: Values for last employment date,
 	SELECT EMPLOYEE_ID
 		, MIN(LEAST(IFNULL(TERMINATION_DATE, CONTRACT_END_DATE),
					IFNULL(CONTRACT_END_DATE,TERMINATION_DATE)) )			AS EMPLOYMENT_END_DATE
 		, MAX(TERMINATION_REASON)											AS t_TERMINATION_REASON
 		, IFNULL(EMPLOYMENT_END_DATE, '9999-12-31') > CURRENT_DATE 			AS ACTIVE_EMPLOYEE
 	FROM BASIC_SNAP
 	GROUP BY EMPLOYEE_ID

 )

SELECT i.EMPLOYEE_ID
	, i.EMPLOYEE_NAME
	, i.EMPLOYEE_NAME_REVERSE
	, i.GENDER
	, i.DEPARTMENT
	, i.SUB_DEPARTMENT
	, i.EXTERNAL_EMPLOYEE
	, i.EMPLOYMENT_TYPE
	, i.WEEKLY_WORKING_HOUR
	, i.EMPLOYMENT_START_DATE
	, m.EMPLOYMENT_END_DATE
	, m.ACTIVE_EMPLOYEE										AS ACTIVE_EMPLOYEE
	, m.t_TERMINATION_REASON	 							AS TERMINATION_REASON
	, MIN(i.VALID_FROM) 									AS VALID_FROM
	, MAX(i.VALID_TO)										AS VALID_TO
	, IFF(MAX(i.VALID_TO)	= '9999-12-31', TRUE, FALSE)	AS ACTIVE_INDICATOR
	, i.HASH_key
FROM ISLANDS i
LEFT JOIN MIN_MAX m USING (EMPLOYEE_ID)
GROUP BY i.EMPLOYEE_ID
	, i.EMPLOYEE_NAME
	, i.EMPLOYEE_NAME_REVERSE
	, i.GENDER
	, i.DEPARTMENT
	, i.SUB_DEPARTMENT
	, i.EXTERNAL_EMPLOYEE
	, i.EMPLOYMENT_TYPE
	, i.WEEKLY_WORKING_HOUR
	, i.EMPLOYMENT_START_DATE
	, m.EMPLOYMENT_END_DATE
	, m.ACTIVE_EMPLOYEE
	, m.t_TERMINATION_REASON
	, i.ISLAND_ID
	, i.HASH_key