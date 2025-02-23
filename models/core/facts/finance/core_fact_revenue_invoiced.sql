SELECT SLI.DATE_KEY												AS DATE_KEY
	, SLI.INVOICE_ID
	, IFNULL(CDD.DEAL_SK, -1)									AS DEAL_SK
	, COALESCE(CDC.COMPANY_SK, CDD.COMPANY_SK, -1) 				AS COMPANY_SK
	, SLI.INVOICE_NUMBER
	, SLI.INVOICE_HEADER
	, COALESCE(SLI.CHARGE_INTERVAL, CDD.PAYMENT_OCCURRENCE, IFF(SLI.RECURRING, 1, 0))	AS PAYMENT_INTERVAL
	, SUM(IFF(SLI.RECURRING, SLI.NET_AMOUNT, 0)) 				AS RECURRING_AMOUNT
	, SUM(IFF(SLI.RECURRING, 0, SLI.NET_AMOUNT)) 				AS NON_RECURRING_AMOUNT
	, SUM(SLI.NET_AMOUNT)										AS TOTAL_AMOUNT
	, CDD.CONTRACT_START_DATE
	, CDD.CONTRACT_END_DATE
	, SLI.INVOICE_PERIOD_FROM
	, SLI.INVOICE_PERIOD_UNTIL
	, SLI.PAID_INDICATOR
	, SLI.PAID_DATE
	, SLI.DUE_DATE
	, SLI.DUNNING_LEVEL
FROM {{ ref('staging_sevdesk_invoices') }} SLI
LEFT JOIN {{ ref('core_dim_deal') }} CDD ON CDD.DEAL_ID = SLI.DEAL_ID AND CDD.ACTIVE_INDICATOR = TRUE
LEFT JOIN {{ ref('core_dim_company') }} CDC ON CDC.SEVDESK_CUSTOMER_ID = SLI.SEVDESK_CUSTOMER_ID
-- NOTE: Temporary measure to ensure Company report is correct (2023-06-14)
GROUP BY SLI.DATE_KEY
	, SLI.INVOICE_ID
	, IFNULL(CDD.DEAL_SK, -1)
	, COALESCE(CDC.COMPANY_SK, CDD.COMPANY_SK, -1)
	, SLI.INVOICE_NUMBER
	, SLI.INVOICE_HEADER
	, COALESCE(SLI.CHARGE_INTERVAL, CDD.PAYMENT_OCCURRENCE, IFF(SLI.RECURRING, 1, 0))
	, CDD.CONTRACT_START_DATE
	, CDD.CONTRACT_END_DATE
	, SLI.INVOICE_PERIOD_FROM
	, SLI.INVOICE_PERIOD_UNTIL
	, SLI.PAID_INDICATOR
	, SLI.PAID_DATE
	, SLI.DUE_DATE
	, SLI.DUNNING_LEVEL