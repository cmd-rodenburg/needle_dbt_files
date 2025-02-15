SELECT
	"Date" 							AS DATE_KEY
	, "Department"					AS DEPARTMENT
	, "Category"					AS CATEGORY
	, "Sub Category"				AS SUB_CATEGORY
	, "Target"						AS TARGET_VALUE
FROM {{ source('flatfiles', 'targets') }}

