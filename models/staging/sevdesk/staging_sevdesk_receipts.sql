{{ config(materialized='table') }}

WITH RAW_RECEIPTS AS (

 SELECT "id"						::BIGINT		AS RECEIPT_ID
	, "voucherDate"					::DATE			AS DATE_KEY
	, "deliveryDate" 				::DATE			AS DELIVERY_DATE
	, "payDate"						::DATE			AS PAY_DATE
	, "paymentDeadline"				::DATE			AS DUE_DATE
	, "supplier":"id"				::BIGINT		AS CONTACT_ID
	, "creditDebit"									AS CREDIT_DEBIT
	, "description"									AS RECEIPT_DESCRIPTION
	, "document":"id"				::BIGINT		AS DOCUMENT_ID
	, "costCentre":"id"				::BIGINT		AS COST_CENTRE_ID
	, "status"										AS PAYMENT_STATUS
	, "voucherType"									AS RECURRENCE
	, "origin":"id" 		 		::BIGINT		AS ORIGIN_ID
	, "iban"										AS IBAN
	, "tags"										AS TAGS
	, "sumNet"				 		::FLOAT			AS NET_AMOUNT
	, "sumTax"				 		::FLOAT			AS TAX_AMOUNT
	, "sumGross"					::FLOAT			AS GROSS_AMOUNT
	, "sumDiscountNet" 				::FLOAT			AS NET_DISCOUNT
	, "sumDiscountGross" 			::FLOAT			AS GROSS_DISCOUNT
	, "paidAmount" 					::FLOAT			AS PAID_AMOUNT
	, "vatNumber"									AS VAT_NUMBER
	, "recurringInterval"							AS RECURRING_INTERVAL
	, "recurringStartDate"			::DATE			AS RECURRING_START_DATE
	, "recurringNextVoucher"		::DATE			AS RECURRING_NEXT_VOUCHER
	, "recurringEndDate"			::DATE			AS RECURRING_END_DATE
	, "recurringLastVoucher"		::DATE			AS RECURRING_LAST_VOUCHER
	FROM {{ source('sevdesk', 'receipts') }}
	-- NOTE: Status 50 = Draft receipts
 	WHERE PAYMENT_STATUS != '50'

)
, RECEIPT_TAGS AS (

	SELECT RECEIPT_ID
 		, LISTAGG(VALUE:"name", '; ')		 AS TAGS
	FROM RAW_RECEIPTS V
		,LATERAL flatten(INPUT => V.TAGS) AS AB
 	GROUP BY RECEIPT_ID

)

, RAW_RECEIPTS_LINE_ITEMS AS (

	SELECT
		"voucher":"id" 			::BIGINT 					AS RECEIPT_ID
		, "id"					::BIGINT					AS RECEIPT_LINE_ITEM_ID
		, "taxRate"				::FLOAT 					AS TAX_PERCENTAGE
		, "sumNetAccounting"	::NUMERIC(10,2)				AS NET_AMOUNT
		, "sumTaxAccounting"	::NUMERIC(10,2)				AS TAX_AMOUNT
		, "sumGrossAccounting"	::NUMERIC(10,2)				AS GROSS_AMOUNT
		, "comment"											AS LINE_ITEM_DESCRIPTION
		,  CASE "accountingType":"id"	::INT
			WHEN 1				THEN 'Other income'
			WHEN 3				THEN 'Sales Tax paid'
			WHEN 18				THEN 'Purchase of material'
			WHEN 25				THEN 'Sales Tax received'
			WHEN 26				THEN 'Revenue'
			WHEN 58				THEN 'Wage / Salary'
			WHEN 74				THEN 'Account management / Card fees'
			WHEN 75				THEN 'Postage'
			WHEN 84				THEN 'Advance and retroactive Sales Tax payment'
			WHEN 91				THEN 'Insurances / Dues'
			WHEN 107			THEN 'Transport & Freight'
			WHEN 681756			THEN 'Office Equipment'
			WHEN 681758			THEN 'Purchased services'
			WHEN 681770			THEN 'Purchased goods'
			WHEN 681772			THEN 'Statutory social security expenses'
			WHEN 681774			THEN 'Salaries'
			WHEN 681776			THEN 'Wages and salaries'
			WHEN 681778			THEN 'Rent/Related services'
			WHEN 681781			THEN 'Insurance premiums'
			WHEN 681782			THEN 'Contributions'
			WHEN 681783			THEN 'Other levies'
			WHEN 681784			THEN 'Advertising expenses'
			WHEN 681785			THEN 'Merchandise, catering and gifts'
			WHEN 681786			THEN 'Employee travel expenses'
			WHEN 681790			THEN 'Purchased services/third-party services'
			WHEN 681791			THEN 'Postage'
			WHEN 681792			THEN 'Communication/phone'
			WHEN 681793			THEN 'Office supplies'
			WHEN 681794			THEN 'Training costs'
			WHEN 681795			THEN 'Legal and consulting expenses'
			WHEN 681798			THEN 'Software, SaaS and license fees'
			WHEN 681799			THEN 'Other operating expenses'
			ELSE 'other' 		END							AS ACCOUNTING_CATEGORY
		, "create"				::TIMESTAMP WITH TIME ZONE	AS CREATE_TIMESTAMP
	FROM {{ source('sevdesk', 'receipt_items') }}

)

select
	RR.RECEIPT_ID
	, RRLI.RECEIPT_LINE_ITEM_ID
	, RRLI.TAX_PERCENTAGE
	, RRLI.NET_AMOUNT
	, RRLI.TAX_AMOUNT
	, RRLI.GROSS_AMOUNT
	, RRLI.LINE_ITEM_DESCRIPTION
	, RRLI.CREATE_TIMESTAMP
	, RR.DATE_KEY
	, RR.DELIVERY_DATE
	, RR.PAY_DATE
	, RR.DUE_DATE
	, RR.CONTACT_ID
	, RR.CREDIT_DEBIT
	, RR.RECEIPT_DESCRIPTION
	, RR.DOCUMENT_ID
	, RR.COST_CENTRE_ID
	, RR.ORIGIN_ID
	, RR.IBAN
	, RR.VAT_NUMBER
	, RR.RECURRING_START_DATE
	, RR.RECURRING_NEXT_VOUCHER
	, RR.RECURRING_END_DATE
	, RR.RECURRING_LAST_VOUCHER
	, RT.TAGS
	, CASE RR.COST_CENTRE_ID
		WHEN 109046				THEN 'Product & Tech'
		WHEN 108291				THEN 'Product & Tech'
		WHEN 89679 				THEN 'Management'
		WHEN 89674 				THEN 'Other'
		WHEN 89670 				THEN 'Sales'
		WHEN 89668 				THEN 'Sales'
		WHEN 89667 				THEN 'Product & Tech'
		WHEN 89666 				THEN 'Sales'
		WHEN 89665 				THEN 'Business operations'
		WHEN 89664 				THEN 'Business operations'
		WHEN 89663 				THEN 'Product & Tech'
		ELSE 'UNKOWN'			END 	AS COST_CENTRE
	, RRLI.ACCOUNTING_CATEGORY
	, CASE PAYMENT_STATUS
		WHEN '100' 	THEN 'Unpaid/Due'
		WHEN '1000' THEN 'Paid'
	END AS PAYMENT_STATUS
	, CASE RECURRENCE
		WHEN 'VOU' THEN 'One-off'
		WHEN 'RV' 	THEN 'Recurring'
	END AS RECURRENCE
	, CASE RECURRING_INTERVAL
		WHEN 'P1Y' 	THEN 'yearly'
		WHEN 'P0Y1M' THEN 'monthly'
		WHEN 'P0Y3M' THEN 'quarterly'
		WHEN 'P0Y6M' THEN 'biannual'
	END AS RECURRING_INTERVAL
FROM RAW_RECEIPTS RR
LEFT JOIN RAW_RECEIPTS_LINE_ITEMS RRLI USING (RECEIPT_ID)
LEFT JOIN RECEIPT_TAGS RT USING (RECEIPT_ID)
