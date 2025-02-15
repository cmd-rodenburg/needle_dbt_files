-- TODO: Switch to snap as source
SELECT
		st.value ->> 'id' AS id
	, pl.id AS pipeline_id
	, pl.object_type
	, st.value ->> 'label' AS stage_name
	, CONCAT( -- add alphabetic enumeration in front of stage names
				'(' -- CHR(97) = 'a' in ASCII
			, CHR(97 + (st.value ->> 'displayOrder')::INT)
			, ') '
			, (st.value ->> 'label')
		) AS stage_category
	, st.value ->> 'archived' = 'true' AS is_archived
	, (st.value ->> 'displayOrder')::INT AS display_order
	, st.value -> 'metadata' ->> 'isClosed' = 'true' AS is_closed
	, st.value -> 'metadata' ->> 'ticketState' AS ticket_state
	, (st.value -> 'metadata' ->> 'probability')::NUMERIC AS probability
	, (st.value -> 'metadata' ->> 'probability')::NUMERIC = 1 AS is_won
	, (st.value ->> 'createdAt')::TIMESTAMP WITH TIME ZONE AS created_at
	, (st.value ->> 'updatedAt')::TIMESTAMP WITH TIME ZONE AS updated_at
FROM {{ source('hubspot', 'pipelines') }} AS pl
  , jsonb_array_elements(pl.stages) AS st
