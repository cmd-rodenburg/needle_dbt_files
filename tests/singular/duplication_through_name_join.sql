-- Test for duplication creation via name join (Solytic 1 / Powerdoo) to HUbspot
-- Should this test fail, change the hubspot name of the duplicated company

SELECT
	COMPANY_NAME,
	COUNT(COMPANY_NAME)
FROM {{ ref("core_dim_company")}}
WHERE   ACTUAL_SOLYTIC_1_SITES > 0
GROUP BY
	COMPANY_NAME
HAVING
	COUNT(COMPANY_NAME) > 1