{{ config(
    tags=["gdpr"]
) }}

WITH email AS (

	SELECT "contact":"id":: BIGINT													AS CONTACT_ID
		, "value"																	AS EMAIL
		, CASE "key":"id"::int
			WHEN 1 THEN 'Privat'
			WHEN 2 THEN 'Arbeit'
			WHEN 3 THEN 'Fax'
			WHEN 4 THEN 'Mobil'
			WHEN 5 THEN '" "'
			WHEN 6 THEN 'Autobox'
			WHEN 7 THEN 'Newsletter'
			WHEN 8 THEN 'Rechnungsadresse'
		END 																		AS EMAIL_TYPE
		, ROW_NUMBER() OVER (PARTITION BY CONTACT_ID ORDER BY "key":"id"::int DESC) AS EMAIL_ORDER
		, COUNT(DISTINCT "value") OVER (PARTITION BY CONTACT_ID) 					AS NUM_EMAILS
	FROM {{ source('sevdesk', 'communication_way') }}
	WHERE "type"  = 'EMAIL'
		AND "value" IS NOT NULL

)

SELECT
	"id"							::BIGINT		AS CONTACT_ID
	, c."customerNumber"			::BIGINT		AS CUSTOMER_NUMBER
	, c."name"										AS CONTACT_NAME
	, IFF(c."name2" != c."name", c."name2", NULL)	AS CONTACT_NAME_SECONDARY
	, e.EMAIL
	, e.NUM_EMAILS
	, c."status"					::INT			AS STATUS
	, c."category":"id"				::BIGINT		AS CATEGORY_ID
	, CASE c."category":"id"		::INT
		WHEN 2 THEN 'Supplier'
		WHEN 3 THEN 'Customer'
		WHEN 4 THEN 'Partner'
		ELSE 'Unknown'
	END 											AS CATEGORY
	, c."vatNumber"									AS VAT_NUMBER
	, c."taxNumber"									AS TAX_NUMBER
	, REPLACE(TRIM(c."bankAccount"), ' ', '')		AS BANK_IBAN
	, c."bankNumber"								AS BIC_NUMBER
	, c."create"	::TIMESTAMP WITH TIME ZONE		AS CREATE_TIMESTAMP
	, c."update"	::TIMESTAMP WITH TIME ZONE		AS UPDATE_TIMESTAMP
FROM {{ source('sevdesk', 'customers')}} c
LEFT JOIN email e  ON e.CONTACT_ID = CAST(c."id" AS BIGINT)
WHERE c."name" not like '%Dummy%'
	-- NOTE: EMAIL ORDER SELECTS THE FIRST EMAIL TO ENSURE THE CORRECT EMAIL AND NO DUPLICATES
	AND (e.email_order = 1 OR e.email_order IS NULL)