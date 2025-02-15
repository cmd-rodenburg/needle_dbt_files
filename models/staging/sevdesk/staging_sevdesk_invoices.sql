{{ config(materialized='table') }}

WITH CANCELLED_INVOICES AS (

	SELECT DISTINCT inv."origin":"id"	::BIGINT	AS CANCELLED_INVOICE_ID
	FROM {{ source('sevdesk', 'invoices')}}	inv
	WHERE inv."invoiceType" = 'SR'

)


SELECT inv."id"			 							::BIGINT				AS INVOICE_ID
	, inv."invoiceDate"								::DATE 					AS DATE_KEY
	-- NOTE: Duplicated invoice number set to invoice number without "-"
	, IFF(inv."invoiceNumber" = '985261055', 'RE3442', inv."invoiceNumber") AS INVOICE_NUMBER
	, inv."header" 															AS INVOICE_HEADER
	, TRY_CAST(inv."customerInternalNote" AS BIGINT)						AS DEAL_ID
	, CASE
		WHEN DEAL_ID IN (4001032430, 4510613736, 4001034808) 	THEN 911300494	-- Kostal
		WHEN DEAL_ID IN (8087853690, 4281079181) 				THEN 3902077376	-- MMS Energy O.E.
		WHEN DEAL_ID IN (5064152459, 5614738703) 				THEN 2355594465	-- Meier-NT GmbH
		WHEN DEAL_ID = 5977443610	 							THEN 2362105093	-- Vaillant
		WHEN DEAL_ID = 1311571210								THEN 6106229900	-- Bos.ten
		WHEN DEAL_ID = 6317679203 								THEN 2362105095	-- KATEK
		WHEN DEAL_ID = 4169790829 								THEN 4437965696	-- EWE
		WHEN DEAL_ID IN (5461088934, 5030169427)				THEN 911617319	-- Solar PLUS Cleaning
		WHEN INVOICE_NUMBER = 'RE-4396'			 				THEN 2884954891	-- Steag Gmbh
	END 											::BIGINT				AS HUBSPOT_COMPANY_ID
	, inv."contact":"id"							::BIGINT				AS CONTACT_ID
	, c."customerNumber"							::BIGINT				AS SEVDESK_CUSTOMER_ID
	, li."id"										::BIGINT 				AS LINE_ITEM_ID
	, li."part":"id"								::BIGINT				AS PRODUCT_ID
	, li."name"																AS PRODUCT_NAME
	, li."quantity"									::FLOAT					AS QUANTITY
	, li."price"									::FLOAT 				AS UNIT_PRICE
	, li."sumDiscount"								::FLOAT 				AS TOTAL_DISCOUNT
	, li."sumNetAccounting"							::FLOAT 				AS NET_AMOUNT
	, li."sumTaxAccounting"							::FLOAT 				AS TAX_AMOUNT
	, li."sumGrossAccounting"						::FLOAT 				AS GROSS_AMOUNT
	, inv."deliveryDate"							::DATE					AS INVOICE_PERIOD_FROM
	, inv."deliveryDateUntil"						::DATE					AS INVOICE_PERIOD_UNTIL
	, inv."status"::INT = 1000												AS PAID_INDICATOR
	, inv."payDate"									::DATE					AS PAID_DATE
	, CASE
		WHEN inv."accountIntervall" = 'P1Y' 	THEN 12
		WHEN inv."accountIntervall" = 'P0Y1M' 	THEN 1
		WHEN inv."accountIntervall" = 'P0Y3M' 	THEN 3
		WHEN inv."accountIntervall" = 'P0Y6M' 	THEN 6
	END 																	AS CHARGE_INTERVAL
	, inv."currency"														AS CURRENCY
	, CASE
		WHEN lower("footText") LIKE '%sepa%' AND lower("footText") LIKE '%den rechnungsbetrag unter angabe der rechnungsnummer%'	THEN NULL
		WHEN lower("footText") LIKE '%sepa%'																						THEN 'SEPA payment'
		WHEN lower("footText") LIKE '%stripe%' OR lower("footText") LIKE '%credit-card%' 											THEN 'credit card'
		WHEN lower("footText") LIKE '%transfer the invoice amount, stating the invoice number, to the account indicated below%'
			OR lower("footText") LIKE '%den rechnungsbetrag unter angabe der rechnungsnummer auf das unten angegebene konto unter%' THEN 'bank wire'
	END 																	AS PAYMENT_TYPE
	, inv."timeToPay"														AS DAYS_TO_PAY_INVOICE
	, DATEADD(day, DAYS_TO_PAY_INVOICE,  DATE_KEY)::DATE 					AS DUE_DATE
	, inv."dunningLevel"													AS DUNNING_LEVEL
	, pr."partNumber" 														AS PRODUCT_NUMBER
	-- NOTE: KOSTAL INVOICES NOT MARKED RECURRING
	, IFF(INVOICE_NUMBER IN ('RE-4617', 'RE-4589', 'RE-4633', 'RE-4666', 'RE-4634', 'RE-4699', 'RE-4711', 'RE-4709') , TRUE, sc.RECURRING_INDICATOR	) ::BOOLEAN AS RECURRING
	, IFF(RECURRING, 'Monitoring recurring', sc.PRODUCT_CATEGORY)			AS REVENUE_CATEGORY
FROM {{ source('sevdesk', 'invoices')}}	inv
LEFT JOIN {{ source('sevdesk', 'invoice_items')}} li ON inv."id" = li."invoice":"id"::BIGINT
LEFT JOIN {{ source('sevdesk', 'products') }} pr ON pr."id" = li."part":"id"
LEFT JOIN {{ source('flatfiles', 'sevdesk_categories') }} sc ON pr."partNumber" = sc.PRODUCT_NUMBER
LEFT JOIN {{ source('sevdesk', 'customers')}} c ON inv."contact":"id" ::BIGINT = c."id"	::BIGINT
	-- NOTE: Filter out draft invoices
WHERE inv."status"::INT != 100
	AND INVOICE_ID NOT IN (SELECT CANCELLED_INVOICE_ID FROM CANCELLED_INVOICES)
	AND  inv."invoiceType" = 'RE'

	UNION

	-- Sevdesk invoices before Jan 2021
SELECT "id"							::BIGINT							AS INVOICE_ID
	, "voucherDate"					::DATE								AS DATE_KEY
	, "description"					::TEXT								AS INVOICE_NUMBER
	, NULL 						 	::TEXT 								AS INVOICE_HEADER
	, NULL							::BIGINT							AS DEAL_ID
	, NULL							::BIGINT							AS HUBSPOT_COMPANY_ID
	, "supplier":"id"				::BIGINT							AS CONTACT_ID
	, NULL							::BIGINT							AS SEVDESK_CUSTOMER_ID
	, NULL							::BIGINT							AS LINE_ITEM_ID
	, NULL							::BIGINT							AS PRODUCT_ID
	, NULL							::TEXT								AS PRODUCT_NAME
	, NULL							::FLOAT								AS QUANTITY
	, NULL							::FLOAT 							AS UNIT_PRICE
	, NULL							::FLOAT 							AS TOTAL_DISCOUNT
	, "sumNet"				 		::FLOAT								AS NET_AMOUNT
	, "sumTax"				 		::FLOAT								AS TAX_AMOUNT
	, "sumGross"					::FLOAT								AS GROSS_AMOUNT
	, NULL							::DATE 								AS INVOICE_PERIOD_FROM
	, NULL							::DATE 								AS INVOICE_PERIOD_UNTIL
	, IFF("status"::INT = 1000, TRUE, FALSE)							AS PAID_INDICATOR
	, "payDate"						::DATE								AS PAID_DATE
	, NULL 							::int 								AS CHARGE_INTERVAL
	, NULL 			 			 	::TEXT 								AS CURRENCY
	, NULL																AS PAYMENT_TYPE
	, NULL																AS DAYS_TO_PAY_INVOICE
	, NULL																AS DUE_DATE
	, NULL																AS DUNNING_LEVEL
	, NULL																AS PRODUCT_NUMBER
	, NULL																AS RECURRING
	, NULL																AS REVENUE_CATEGORY
FROM {{ source('sevdesk', 'receipts') }}
where "creditDebit"	= 'D'
 	AND REGEXP_LIKE("description", '^[0-9\.]+$')
	AND "description" NOT IN ('3144', '10470000046230001002')
	AND "voucherDate" ::DATE <= '2021-01-01'
