SELECT
	DATE_KEY                      	AS DATE_KEY
	, DEPARTMENT                    AS DEPARTMENT
	, CATEGORY                      AS CATEGORY
	, SUB_CATEGORY                  AS SUB_CATEGORY
	, TARGET_VALUE                  AS TARGET_VALUE
FROM {{ ref('staging_flatfiles_target') }}
