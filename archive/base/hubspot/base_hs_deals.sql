WITH snap_deals AS (

	SELECT * FROM {{ ref('snap_hubspot_deals') }}
	-- ignore deleted deals - if they are deleted, they won't appear in the source again
	WHERE id IN (SELECT id FROM {{ source("hubspot", "deals") }})

), solytic_test_deals AS (

	SELECT value::BIGINT AS deal_id
	FROM {{ ref("hs_solytic_companies") }}, JSONB_ARRAY_ELEMENTS(associated_deals)

), features_property_options AS (

	SELECT
			CASE -- correct the first "valid_from"
				WHEN valid_from = MIN(valid_from) OVER (PARTITION BY id)
					THEN '1900-01-01'::TIMESTAMP WITH TIME ZONE
				ELSE valid_from
			END AS valid_from
		, COALESCE(valid_to, NOW() + INTERVAL '30 day') AS valid_to
		, options
	FROM {{ ref('base_hs_properties') }}
	WHERE name = 'highlight_features_2_0'

), current_feature_label AS (
	-- Get the latest list of value-label pairs for all features, including deprecated

	WITH extracted_labels AS (
		SELECT
				jae.value ->> 'value' AS option_value
			, jae.value ->> 'label' AS option_label
			, fpo.valid_to
		FROM features_property_options AS fpo
			, jsonb_array_elements(fpo.options) AS jae
	), add_rn AS (
		SELECT *, ROW_NUMBER() OVER (PARTITION BY option_value ORDER BY valid_to DESC) AS rn
		FROM extracted_labels
	)
	SELECT option_value, option_label
	FROM add_rn
	WHERE rn = 1

), transform AS (

	SELECT
			id::BIGINT
	  , dbt_valid_from::TIMESTAMP WITH TIME ZONE AS valid_from
	  , dbt_valid_to::TIMESTAMP WITH TIME ZONE AS valid_to
	  , dbt_valid_to IS NULL AS is_current
		, archived AS is_archived
		, NULLIF(amount, '')::NUMERIC AS amount
		, NULLIF(amount_in_home_currency, '')::NUMERIC AS amount_in_home_currency
		, business_type
		, (
        SELECT JSONB_AGG((value->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_contacts)
        WHERE value ->> 'type' = 'deal_to_contact'
      ) AS associated_contacts
		, client_momentum
		, client_need
		, NULLIF(contract_start_date, '')::DATE AS contract_start_date
		, NULLIF(contract_end_date, '')::DATE AS contract_end_date
	  , current_contract_end
		, NULLIF(days_to_close, '')::BIGINT AS days_to_close
		, dealname
		, dealstage -- keep for namesake as some people may actively look for this field
		, dealstage AS pipeline_stage_id
		, dealtype
		, b2b_deal_kategorie											as deal_category
	  , deal_persona
		, hs_analytics_source AS deal_original_source_type
		, hs_analytics_source_data_1 AS deal_original_source_data_1
		, hs_analytics_source_data_2 AS deal_original_source_data_2
	  , closed_lost_reason
		, lost_reason
		, NULLIF(expected_deal_closing_month, '')::DATE AS expected_deal_closing_month
		, NULLIF(hubspot_owner_assigneddate, '')::TIMESTAMP WITH TIME ZONE
	    AS hubspot_owner_assigned_at
		, NULLIF(hubspot_owner_id, '')::BIGINT AS hubspot_owner_id
		, NULLIF(hubspot_team_id, '')::BIGINT AS hubspot_team_id
	  , offer_expiration_date
		, pipeline
		, portfolio_size_capacity_ AS portfolio_size_capacity
		, CASE
				WHEN NULLIF(portfolio_size_in_mwp, '') IS NULL THEN NULL
				WHEN portfolio_size_in_mwp::NUMERIC < 0.02 THEN '< 20 kWp'
				WHEN portfolio_size_in_mwp::NUMERIC < 1 THEN '20 kWp - 1 MWp'
				WHEN portfolio_size_in_mwp::NUMERIC < 10 THEN '1 MWp - 10 MWp'
				WHEN portfolio_size_in_mwp::NUMERIC < 100 THEN '10 MWp - 100 MWp'
				ELSE '>= 100 MWp'
			END AS dealsize_category
		, NULLIF(portfolio_size_in_mwp, '')::NUMERIC AS portfolio_size_in_mwp
		, portfolio_size_units_ -- deprecated but potentially in use in reports
		, portfolio_size_units_ AS portfolio_size_units
		, COALESCE(project_based = 'true', FALSE) AS is_project_based
		, COALESCE(lost_reason LIKE '%Internal Test%', FALSE) -- this is a checkbox
			OR (id::BIGINT IN (SELECT deal_id FROM solytic_test_deals))
			OR LOWER(dealname) LIKE '%solytic%'
			OR COALESCE(hubspot_owner_id = '50699536', FALSE)
			AS is_test_deal
		, COALESCE(test_deal = 'true', FALSE) AS is_poc_deal -- "test" here means POC
		, COALESCE(whitelabel_basis = 'true', FALSE) AS is_whitelabel_basis
		, NULLIF(notes_last_contacted, '')::TIMESTAMP WITH TIME ZONE
	    AS notes_last_contacted_at
		, COALESCE(NULLIF(num_contacted_notes, '')::INT, 0) AS num_contacted_notes
	  , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
		, "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at

	FROM snap_deals AS sd

)

SELECT * FROM transform
