 -- NOTE: match with Personio reverse naming to get a uniform name per person

 WITH reverse_naming AS(

	SELECT
		ID												AS EMPLOYEE_ID
		, DATE_KEY						 ::DATE			AS DATE_KEY
		, CASE EMPLOYEE_NAME
			WHEN 'Rahoui Jassmine'					THEN 'Rahoui Yassmine'
			WHEN 'D''Souza Siddharth Rudol'			THEN 'D''Souza Siddharth'
			WHEN 'Dawood Ranin Bassam'				THEN 'Dawood Ranin'
			WHEN 'Nazareth Santos Mario Hele' 		THEN 'Santos Mario'
			WHEN 'Petri Christoph Ronal'			THEN 'Petri Christoph Roland'
			WHEN 'Teichmann Tom-Niklas' 			THEN 'Teichmann Tom'
			WHEN 'Perenyi Konrad'					THEN 'Perényi Konrad'
			ELSE EMPLOYEE_NAME END 						AS EMPLOYEE_NAME_REVERSE
		, ROUND(SALARY::NUMERIC , 2) 					AS GROSS_SALARY
		, ROUND(SOCIAL::NUMERIC , 2) 					AS SOCIAL_SECURITY
	FROM {{ source('flatfiles', 'expenses_personnel') }}

 )

 SELECT
 	rn.EMPLOYEE_ID
	, rn.DATE_KEY
	, pe.EMPLOYEE_NAME
	, rn.GROSS_SALARY
	, rn.SOCIAL_SECURITY
FROM reverse_naming rn
LEFT JOIN  {{ ref('staging_personio_employees') }} pe ON rn.EMPLOYEE_NAME_REVERSE = pe.EMPLOYEE_NAME_REVERSE AND ACTIVE_INDICATOR = TRUE