WITH source AS (

  SELECT * FROM {{ source('sevdesk', 'invoices')}}

), renamed AS (

  SELECT
    id::BIGINT AS sevdesk_invoice_id
    , CASE
    --here we manually correct one customer
        WHEN (contact ->> 'id')::BIGINT = 34793433 THEN 37282245
        ELSE (contact ->> 'id')::BIGINT
      END AS sevdesk_customer_id
    ,"invoiceNumber" as sevdesk_invoice_number
    , CASE
        WHEN "customerInternalNote" = 'Sascha Severin' THEN NULL::BIGINT
        ELSE "customerInternalNote"::BIGINT
      END AS hubspot_deal_id
    , "invoiceDate"::DATE AT TIME ZONE 'utc' AT
      TIME ZONE '{{ var('gemma:dates:timezone') }}' AS invoice_date
    , "accountStartDate"::DATE AT TIME ZONE 'utc' AT
      TIME ZONE '{{ var('gemma:dates:timezone') }}' AS account_start_date
    , "accountNextInvoice"::DATE AT TIME ZONE 'utc' AT
      TIME ZONE '{{ var('gemma:dates:timezone') }}' AS next_invoice_date
    , CASE
        WHEN "accountIntervall" = 'P1Y' THEN 'yearly'
        WHEN "accountIntervall" = 'P0Y1M' THEN 'monthly'
        WHEN "accountIntervall" = 'P0Y3M' THEN 'quarterly'
        WHEN "accountIntervall" = 'P0Y6M' THEN 'biannual'
      END AS charge_interval
    , "header" AS invoice_header
    , currency
    , "sumNetAccounting"::FLOAT AS invoice_net_amount
    , "sumTaxAccounting"::FLOAT AS invoice_tax_amount
    , "sumGrossAccounting"::FLOAT AS invoice_gross_amount
    , "sumNetForeignCurrency"::FLOAT AS invoice_net_amount_foreign_currency
    , "sumTaxForeignCurrency"::FLOAT AS invoice_tax_amount_foreign_currency
    , "sumGrossForeignCurrency"::FLOAT AS invoice_gross_amount_foreign_currency
    , "addressName" As deprecated_address_name
    , CASE WHEN status::INT = 1000 THEN True ELSE False END AS is_paid

  FROM source
  --removing draft invoices
  WHERE status::INT != 100

), numbered AS (

  SELECT *
  , ROW_NUMBER() OVER ( PARTITION BY sevdesk_customer_id, hubspot_deal_id ORDER BY invoice_date DESC ) AS last_first
  , ROW_NUMBER() OVER ( PARTITION BY sevdesk_customer_id, hubspot_deal_id ORDER BY invoice_date ASC ) AS earliest_first

  FROM renamed

)

SELECT * FROM numbered
