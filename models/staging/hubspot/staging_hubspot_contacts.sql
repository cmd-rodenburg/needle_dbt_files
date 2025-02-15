-- NOTE: THE CONTACTS STAGING TABLE IS COMBINED FROM TWO SOURCES DUE TO GDPR

with contacts as (

 select * from {{ source('hubspot_engagements', 'contacts') }}
 where "archived" = FALSE

	)
, hs_clean as (

	SELECT "id"					::BIGINT				AS HUBSPOT_CONTACT_ID
		, "associatedcompanyid"							AS HUBSPOT_COMPANY_ID
		, "firstname"									AS FIRST_NAME
		, "lastname"									AS LAST_NAME
		, "jobtitle"									AS JOB_TITLE
		, "job_function"								AS JOB_FUNCTION
		, "email"										AS EMAIL
		, IFNULL("sent_demo_request", FALSE)::boolean	AS DEMO_REQUEST
		, "hs_language"									AS CONTACT_LANGUAGE
		, "ip_country"									AS CONTACT_COUNTRY
		, "ip_city"										AS CONTACT_CITY
		, "hubspotscore"								AS HUBSPOT_SCORE
		, "lifecyclestage"								AS LIFE_CYCLE_STAGE
		, "portal_user"									AS PORTAL_USER
		, "createdAt"				::DATE				AS DATE_KEY
	FROM contacts

)
-- NOTE: IMPORT OPERATIONAL CONTACT DATA
, op_clean AS (

	SELECT mu.ID										AS OPERATIONAL_USER_ID
		, mu.EMAIL 										AS USER_EMAIL
		, mu.DEFAULTUSERWHITELABELID					AS WHITELABEL_ID
		, mu.EMAILCONFIRMED								AS EMAIL_CONFIRMED
		, mu.LOCKOUTENABLED								AS LOCKOUT_ENABLED
		, mu.ISBLOCKED									AS BLOCKED
		, mu.CUSTOMERID									AS OPERATIONAL_COMPANY_ID
		, mu.CREATEDAT			::DATE					AS CREATED_AT
		, mu.LASTLOGIN			::DATE					AS LAST_LOGIN
		, CASE
				WHEN mu.EMAIL LIKE '%solytic.com' THEN SPLIT_PART(mu.EMAIL, '@', 1)
				WHEN SPLIT_PART(mu.USERNAME, '@', 2) IS NULL THEN mu.USERNAME
				ELSE CONCAT(mu.ID, '@', SPLIT_PART(mu.EMAIL, '@', 2))
		END 											AS USERNAME
	FROM {{ source('operation', 'user') }} mu

)

-- NOTE: MERGE OPERATIONAL AND HUBSPOT CONTACTS OVER EMAIL
SELECT DISTINCT
	HS.HUBSPOT_CONTACT_ID
	, mu.OPERATIONAL_USER_ID
	, HS.HUBSPOT_COMPANY_ID
	, mu.OPERATIONAL_COMPANY_ID
	, mu.CREATED_AT
	, HS.CONTACT_LANGUAGE
	, HS.CONTACT_COUNTRY
	, HS.CONTACT_CITY
	, mu.EMAIL_CONFIRMED
	, HS.JOB_TITLE
	, HS.JOB_FUNCTION
	, HS.LIFE_CYCLE_STAGE
	, HS.HUBSPOT_SCORE
	, HS.DEMO_REQUEST
	, HS.PORTAL_USER
	, mu.LAST_LOGIN
	, mu.BLOCKED
	, mu.LOCKOUT_ENABLED
	, HS.DATE_KEY
	, IFNULL(mu.USER_EMAIL, HS.EMAIL) LIKE '%solytic.com'	AS SOLYTIC_EMPLOYEE
	, mu.USERNAME
FROM hs_clean HS
FULL OUTER JOIN op_clean mu ON HS.EMAIL = mu.USER_EMAIL