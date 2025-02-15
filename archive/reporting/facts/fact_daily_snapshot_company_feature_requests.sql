WITH daily_company_data AS (

  {{ get_daily_table_from_snap_base(ref("base_hs_companies"), "id") }}

), union_need_types AS (

  SELECT
      snap_id
    , valid_date
    , id AS company_id
    , is_current
    , requested_must_have_features AS requested_features
    , 'Must-have' AS feature_need_type
  FROM daily_company_data
  WHERE NOT requested_must_have_features IS NULL

  UNION ALL

  SELECT
      snap_id
    , valid_date
    , id AS company_id
    , is_current
    , requested_nice_to_have_features AS requested_features
    , 'Nice-to-have' AS feature_need_type
  FROM daily_company_data
  WHERE NOT requested_nice_to_have_features IS NULL

), regular_features AS (

  SELECT
      snap_id
    , valid_date
    , company_id
    , is_current
    , feature
    , SPLIT_PART(feature, ' - ', 1) AS feature_group
    , TRIM(RIGHT(feature, LENGTH(feature) - 3 - LENGTH(SPLIT_PART(feature, ' - ', 1))))
      AS feature_name
    , CASE
        WHEN LOWER(feature) LIKE '% - other' THEN 'other'
        ELSE 'standard'
      END AS feature_description_type
    , feature_need_type
  FROM union_need_types, UNNEST(STRING_TO_ARRAY(requested_features, ';')) AS feature

), add_other_features AS (

  SELECT * FROM regular_features

  -- add the free-text field data as different feature type to the list

  -- must-have
  UNION ALL
  SELECT
      snap_id
    , valid_date
    , id AS company_id
    , is_current
    , feature
    , 'other_requested_feature_specifications' AS feature_group
    , feature AS feature_name
    , 'other_requested_feature_specifications' AS feature_description_type
    , 'Must-have' AS feature_need_type
  FROM daily_company_data
    , UNNEST(STRING_TO_ARRAY(other_requested_must_have_feature_specifications, ';'))
      AS feature
  WHERE NOT other_requested_must_have_feature_specifications IS NULL

  -- nice-to-have
  UNION ALL
  SELECT
      snap_id
    , valid_date
    , id AS company_id
    , is_current
    , feature
    , 'other_requested_feature_specifications' AS feature_group
    , feature AS feature_name
    , 'other_requested_feature_specifications' AS feature_description_type
    , 'Nice-to-have' AS feature_need_type
  FROM daily_company_data
    , UNNEST(STRING_TO_ARRAY(other_requested_nice_to_have_feature_specifications, ';'))
      AS feature
  WHERE NOT other_requested_nice_to_have_feature_specifications IS NULL

), final_transforms AS (

  SELECT
      *
    , CONCAT(feature_group, ' - ', feature_name) AS feature_sub_group
  FROM add_other_features

)

SELECT * FROM final_transforms
