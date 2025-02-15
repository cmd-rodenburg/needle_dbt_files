{{ config(tags=["gdpr"]) }}

SELECT mu.ID														AS USER_ID
	, CASE
			WHEN mu.EMAIL  LIKE '%solytic.com' THEN SPLIT_PART(mu.EMAIL, '@', 1)
			WHEN SPLIT_PART(mu.USERNAME, '@', 2) IS NULL THEN mu.USERNAME
			ELSE CONCAT(mu.ID, '@', SPLIT_PART(mu.EMAIL, '@', 2))
	END 														AS USERNAME
	, mu.CONTACTID
	, mu.DEFAULTUSERWHITELABELID
	, mu.EMAILCONFIRMED
	, mu.LOCKOUTENABLED
	, mu.LOCKOUTEND
	, mu.ISBLOCKED
	, mu.CUSTOMERID				AS COMPANY_ID
	, mu.CREATEDAT
	, mu.LASTLOGIN
	, mu.EMAIL  LIKE '%solytic.com' AS SOLYTIC_EMPLOYEE
FROM {{ source('operation', 'user') }} mu
