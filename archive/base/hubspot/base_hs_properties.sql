WITH properties_snapshot AS (

  SELECT * FROM {{ ref("snap_hubspot_properties") }}

), transforms AS (

  SELECT
      id
    , dbt_valid_from::TIMESTAMP WITH TIME ZONE AS valid_from
    , dbt_valid_to::TIMESTAMP WITH TIME ZONE AS valid_to
    , dbt_valid_to IS NULL AS is_current
    , archived AS is_archived
    , object_type
    , name
    , label
    , type
    , "fieldType" AS field_type
    , description
    , "groupName" AS group_name
    , options::JSONB AS options
    , "displayOrder" AS display_order
    , calculated AS is_calculated
    , "externalOptions" AS external_options
    , "hasUniqueValue" AS has_unique_value
    , hidden AS is_hidden
    , "modificationMetadata" AS modification_metadata
    , "formField" AS is_form_field
    , "createdUserId"::BIGINT AS created_by
    , "updatedUserId"::BIGINT AS updated_by
    , "referencedObjectType" AS referenced_object_type
    , "showCurrencySymbol" AS show_currency_symbol
    , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
  	, "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at

  FROM properties_snapshot

)

SELECT * FROM transforms
