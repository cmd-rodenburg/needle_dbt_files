WITH snap_contacts AS (

	SELECT * FROM {{ ref('snap_hubspot_contacts') }}

), transform AS (

  SELECT
      id::bigint
    , dbt_valid_from::TIMESTAMP WITH TIME ZONE AS valid_from
    , dbt_valid_to::TIMESTAMP WITH TIME ZONE AS valid_to
    , dbt_valid_to IS NULL AS is_current
    , archived AS is_archived
    , associatedcompanyid AS associated_company_id
    , email
    , hs_email_domain AS email_domain
    , firstname
    , lastname
    , gender

    , source
    , hs_analytics_source AS original_source
    , hs_analytics_source_data_1 AS original_source_1
    , hs_analytics_source_data_2 AS original_source_2
    , hs_latest_source AS latest_source
    , hs_latest_source_data_1 AS latest_source_1
    , hs_latest_source_data_2 AS latest_source_2

    , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
    , "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at
  FROM snap_contacts

)

SELECT * FROM transform
