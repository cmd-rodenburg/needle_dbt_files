WITH source AS (

  SELECT * FROM {{ source('sevdesk', 'invoice_items') }}

), invoices AS (

  SELECT * FROM {{ ref('base_sevdesk_invoices') }}

), renamed AS (

  SELECT
    id::BIGINT AS sevdesk_invoice_item_id
    , (invoice ->> 'id')::BIGINT AS sevdesk_invoice_id
    , (part ->> 'id')::BIGINT AS sevdesk_product_id
    , name
    , quantity::FLOAT
    , price::FLOAT AS unit_price
    , "sumNetAccounting"::FLOAT AS item_net_amount
    , "sumTaxAccounting"::FLOAT AS item_tax_amount
    , "sumGrossAccounting"::FLOAT AS item_gross_amount
    , "sumNetForeignCurrency"::FLOAT AS item_net_amount_foreign_currency
    , "sumTaxForeignCurrency"::FLOAT AS item_tax_amount_foreign_currency
    , "sumGrossForeignCurrency"::FLOAT AS item_gross_amount_foreign_currency
    , update::TIMESTAMP WITH TIME ZONE AS updated_at

    FROM source

), final AS (


  SELECT
    renamed.*
    , invoices.sevdesk_customer_id
    , invoices.hubspot_deal_id
    , invoices.invoice_date
    , CASE
      WHEN invoices.charge_interval = 'monthly'  THEN renamed.item_net_amount
      WHEN invoices.charge_interval = 'yearly'   THEN renamed.item_net_amount/12
      WHEN invoices.charge_interval = 'biannual' THEN renamed.item_net_amount/6
      WHEN invoices.charge_interval = 'quarterly'THEN renamed.item_net_amount/3
      END AS monthly_net_amount
    , ROW_NUMBER() OVER ( PARTITION BY sevdesk_invoice_id ORDER BY updated_at DESC ) AS last_first
    , ROW_NUMBER() OVER ( PARTITION BY sevdesk_invoice_id ORDER BY updated_at ASC ) AS earliest_first

  FROM renamed
   LEFT JOIN invoices
     USING(sevdesk_invoice_id)
  --filtering out draft invoices
  WHERE invoices.sevdesk_invoice_id IS NOT NULL

)

SELECT * FROM final
