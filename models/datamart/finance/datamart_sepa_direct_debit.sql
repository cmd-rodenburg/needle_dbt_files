-- NOTE: CURRENT OUTSTANDING SEPA DIRECT DEBITS

SELECT
    i.DATE_KEY                                      ::DATE	AS INVOICE_DATE
    , i.INVOICE_NUMBER
    , i.INVOICE_HEADER
    , SUM(i.GROSS_AMOUNT*100) 	                    ::Int	AS AMOUNT
    , DATEADD(day, i.DAYS_TO_PAY_INVOICE, i.DATE_KEY)       AS DUE_DATE
    , d.ACCOUNT_HOLDER 									    AS CUSTOMER_NAME
    , d.IBAN
    , d.CLOSE_DATE										    AS MANDATE_DATE
    , c.CUSTOMER_NUMBER + 1000000							AS CUSTOMER_NUMBER
FROM {{ ref('staging_sevdesk_invoices') }} i
LEFT JOIN {{ ref('staging_hubspot_deals') }} d ON i.DEAL_ID = d.DEAL_ID AND ACTIVE_INDICATOR = TRUE
LEFT JOIN {{ ref('staging_sevdesk_contacts') }} c USING(CONTACT_ID)
WHERE i.PAID_INDICATOR = FALSE
    AND i.CURRENCY = 'EUR'
	AND d.PAYMENT_TYPE = 'SEPA payment'
GROUP BY
    i.DATE_KEY
    , i.INVOICE_NUMBER
    , i.INVOICE_HEADER
    , i.DAYS_TO_PAY_INVOICE
    , d.IBAN
    , d.CLOSE_DATE
    , d.ACCOUNT_HOLDER
    , c.CUSTOMER_NUMBER