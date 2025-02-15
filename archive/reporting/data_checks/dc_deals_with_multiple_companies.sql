-- TODO: Once this check returns no rows, add a test to be notified if that changes
WITH companies AS (

  SELECT * FROM {{ ref("dim_companies") }}

), unnested AS (

  SELECT
      co.id AS company_id
    , ad.value AS deal_id
    , COUNT(*) OVER (PARTITION BY ad.value) AS deal_company_count
  FROM companies AS co
    , JSONB_ARRAY_ELEMENTS(co.associated_deals) AS ad
  WHERE NOT co.associated_deals IS NULL

), final AS (

  SELECT
      deal_id
    , company_id
    , deal_company_count
  FROM unnested
  WHERE deal_company_count > 1
  ORDER BY deal_company_count DESC, deal_id ASC

)

SELECT * FROM final
