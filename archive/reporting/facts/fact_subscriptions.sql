WITH invoice_items AS (

    SELECT * FROM {{ ref('base_sevdesk_invoice_items') }}

), invoices AS (

    SELECT * FROM {{ ref('base_sevdesk_invoices') }}

), deals AS (

    SELECT * FROM {{ ref('base_hs_deals') }}

), companies AS (

    SELECT * FROM {{ ref('base_hs_companies') }}

), monthly_amounts_summed AS (

    SELECT
      invoice_items.sevdesk_invoice_id
      , SUM(invoice_items.monthly_net_amount) AS monthly_amount

    FROM invoice_items
    --recurring
    WHERE invoice_items.name LIKE 'Solytic Monitoring SaaS%'

    GROUP BY 1

), subscriptions AS (

    SELECT
      invoices.hubspot_deal_id
      , invoices.sevdesk_customer_id
      , deals.contract_start_date
    --TODO what to do when the Invoice is Stornorechtung but the contract is not ended?
      , deals.contract_end_date
      , monthly_amounts_summed.monthly_amount

    FROM monthly_amounts_summed
      LEFT JOIN invoices
       USING(sevdesk_invoice_id)
    --TODO bring in invoices that do not have a deal
      LEFT JOIN deals
       ON invoices.hubspot_deal_id = deals.id

    WHERE deals.is_current
      AND NOT(deals.is_archived)
      AND invoices.last_first = 1

), companies_added AS (

    SELECT
        companies.id AS hubspot_company_id
      , associated_deals.value::BIGINT AS hubspot_deal_id
      , ROW_NUMBER() OVER ( PARTITION BY companies.id, associated_deals.value ) AS rn
    FROM companies
    , JSONB_ARRAY_ELEMENTS(companies.associated_deals) AS associated_deals
      WHERE NOT companies.associated_deals IS NULL


), final AS (

  SELECT
  subscriptions.*
  , companies_added.hubspot_company_id
  , companies_added.hubspot_deal_id AS subscription_id

  FROM subscriptions
    LEFT JOIN companies_added
      USING(hubspot_deal_id)
  WHERE companies_added.rn = 1

)

SELECT * FROM final
