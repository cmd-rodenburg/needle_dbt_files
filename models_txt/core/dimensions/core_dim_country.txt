-- NOTE: Only select the english version of all countries

SELECT DENSE_RANK () OVER (ORDER BY COUNTRY_ID)		AS COUNTRY_SK
	, COUNTRY_ID
	, COUNTRY_NAME
	, COUNTRY_GEOGRAPHY
FROM  {{ ref('staging_flatfiles_countries') }} sfc
WHERE "LANGUAGE" = 'en'

UNION ALL

-- DEFAULT UNKNOWN VALUE
SELECT -1 											AS COUNTRY_SK
	, -1											AS COUNTRY_ID
	, 'UNKNOWN' 									AS COUNTRY_NAME
	, NULL 											AS COUNTRY_GEOGRAPHY