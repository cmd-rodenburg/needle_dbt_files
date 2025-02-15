
{% set portals_query %}
  -- Get all values of the portal__used semicolon-separated values list
  SELECT DISTINCT portals
  FROM {{ ref('snap_hubspot_companies') }}
    , UNNEST(STRING_TO_ARRAY(portal__used, ';')) AS portals
{% endset %}

{% if execute %}
  {% set portals = run_query(portals_query).columns[0].values() %}
{% else %}
  {% set portals = [] %}
{% endif %}

WITH snap_companies AS (

    SELECT * FROM {{ ref('snap_hubspot_companies') }}
    -- ignore deleted (incl. merged) companies
    WHERE id IN (SELECT id FROM {{ source("hubspot", "companies") }})

), features_property_options AS (

  SELECT
			CASE -- correct the first "valid_from"
				WHEN valid_from = MIN(valid_from) OVER (PARTITION BY id)
					THEN '1900-01-01'::TIMESTAMP WITH TIME ZONE
				ELSE valid_from
			END AS valid_from
		, COALESCE(valid_to, NOW() + INTERVAL '30 day') AS valid_to
		, options
    , name AS property_name
	FROM {{ ref('base_hs_properties') }}
	WHERE name IN ('must_have_features', 'nice_to_have_features')

), current_feature_label AS (
	-- Get the latest list of value-label pairs for all features, including deprecated

	WITH extracted_labels AS (
		SELECT
				jae.value ->> 'value' AS option_value
			, jae.value ->> 'label' AS option_label
			, fpo.valid_to
      , fpo.property_name
		FROM features_property_options AS fpo
			, jsonb_array_elements(fpo.options) AS jae
	), add_rn AS (
		SELECT
        *
      , ROW_NUMBER() OVER (
        PARTITION BY property_name, option_value
        ORDER BY valid_to DESC
      ) AS rn
		FROM extracted_labels
	)
	SELECT property_name, option_value, option_label
	FROM add_rn
	WHERE rn = 1

), transforms AS (

  SELECT
      id::BIGINT
    , name
    , dbt_valid_from::TIMESTAMP WITH TIME ZONE AS valid_from
    , dbt_valid_to::TIMESTAMP WITH TIME ZONE AS valid_to
    , dbt_valid_to IS NULL AS is_current
    , archived AS is_archived
    , phone
    , address
    , city
    , zip
    , state
    , country
    , timezone
    , NULLIF(annualrevenue, '')::NUMERIC AS annual_revenue
    , NULLIF(total_revenue, '')::NUMERIC AS total_revenue
    , description
    , domain
    , facebook_company_page
    , linkedin_company_page
    , twitterhandle
    , founded_year
    , industry
    , is_public = 'true' AS is_public
    , lifecyclestage
    , numberofemployees AS number_of_employees

    , NULLIF(mwp__geplant, '')::NUMERIC AS mwp_planned
    , NULLIF(mwp__aktuell, '')::NUMERIC AS mwp_current
    , portal__used AS portals_used

    -- one boolean for each possible portal
    {% for portal in portals %}
      , COALESCE('{{ portal }}' = ANY(STRING_TO_ARRAY(portal__used, ';')), FALSE)
        AS is_portal_{{ portal | replace(".", "_") }}_used
    {% endfor %}

    , hardware_match
    , CASE
        WHEN hardware_match = 'Fully (100%)' THEN '(a) Fully (100%)'
        WHEN hardware_match = 'Mostly (100% - 75%)' THEN '(b) Mostly (100% - 75%)'
        WHEN hardware_match = 'Partly (75% - 50%)' THEN '(c) Partly (75% - 50%)'
        WHEN hardware_match = 'Insufficient (<50%)' THEN '(d) Insufficient (<50%)'
        ELSE '(e) Not known'
      END AS hardware_match_category
    , hardware_installed__datalogger_inverter___geklont_ AS hardware_installed
    , feature_match
		, CASE
				WHEN feature_match = 'Fully' THEN '(a) Fully (100%)'
				WHEN feature_match = 'Mostly' THEN '(b) Mostly (100%-75%)'
				WHEN feature_match = 'Partly' THEN '(c) Partly (75%-50%)'
				WHEN feature_match = 'Insufficient (<50%)' THEN '(d) Insufficient (<50%)'
				ELSE '(e) Not known'
			END AS feature_match_category

    , hs_analytics_source AS original_source_type
    , hs_analytics_source_data_1 AS original_source_data_1
    , hs_analytics_source_data_2 AS original_source_data_2

    , NULLIF(hubspot_team_id, '')::BIGINT AS hubspot_team_id
    , NULLIF(hubspot_owner_id, '')::BIGINT AS hubspot_owner_id

    , (
        SELECT JSONB_AGG((value->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_contacts)
        WHERE value->>'type' = 'company_to_contact'
      ) AS associated_contacts
    , (
        SELECT JSONB_AGG((value->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_deals)
        WHERE value->>'type' = 'company_to_deal'
      ) AS associated_deals

    , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
  	, "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at

    /*
		 * The requested features logic is not trivial and is added at the and as an
		 * additional column is added in a downstram CTE. Notably, whether a deal has
		 * "other" requested features depends on whether it has a feature with the suffix
		 * "- Other", whereas the other_requested_features field is the specification of
		 * such features.
		 */

    -- Must-have features
    , must_have_features
    , must_have_other_features
		, NOT NULLIF(must_have_features, '') IS NULL AS has_must_have_feature_requests
		, NULLIF(must_have_other_features, '')
      AS other_requested_must_have_feature_specifications
		, NOT NULLIF(must_have_other_features, '') IS NULL
			AS has_other_must_have_feature_request_specified

		, (
				SELECT ARRAY_AGG(vals.option_label)
				FROM UNNEST(STRING_TO_ARRAY(NULLIF(must_have_features, ''), ';')) AS u
					LEFT JOIN current_feature_label AS vals
						ON vals.option_value = u
            AND vals.property_name = 'must_have_features'
			) AS requested_must_have_features_array

    -- Nice2have features
    , nice_to_have_features
    , nice_to_have_other_features
    , NOT NULLIF(nice_to_have_features, '') IS NULL AS has_nice_to_have_feature_requests
		, NULLIF(nice_to_have_other_features, '')
      AS other_requested_nice_to_have_feature_specifications
		, NOT NULLIF(nice_to_have_other_features, '') IS NULL
			AS has_other_nice_to_have_feature_request_specified

		, (
				SELECT ARRAY_AGG(vals.option_label)
				FROM UNNEST(STRING_TO_ARRAY(NULLIF(nice_to_have_features, ''), ';')) AS u
					LEFT JOIN current_feature_label AS vals
						ON vals.option_value = u
            AND vals.property_name = 'nice_to_have_features'
			) AS requested_nice_to_have_features_array

  FROM snap_companies

), add_other_feature_boolean AS (

  SELECT
      *
    , has_must_have_feature_requests OR has_nice_to_have_feature_requests
      AS has_any_feature_requests
    , has_other_must_have_feature_request_specified
      OR has_other_nice_to_have_feature_request_specified
      AS has_any_other_feature_request_specified
    , ARRAY_TO_STRING(requested_must_have_features_array, ';')
      AS requested_must_have_features
    , (
        SELECT COUNT(*) > 0
        FROM UNNEST(requested_must_have_features_array) AS rf
        WHERE LOWER(rf) LIKE '% - other'
      ) AS has_other_must_have_feature_requests

    , ARRAY_TO_STRING(requested_nice_to_have_features_array, ';')
      AS requested_nice_to_have_features
		, (
				SELECT COUNT(*) > 0
				FROM UNNEST(requested_nice_to_have_features_array) AS rf
				WHERE LOWER(rf) LIKE '% - other'
			) AS has_other_nice_to_have_feature_requests

  FROM transforms

)

SELECT * FROM add_other_feature_boolean
