-- TODO: switch to snap as source
SELECT
		id
	, label
	, "displayOrder" AS display_order
	, "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
	, "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at
	, archived AS is_archived
	, object_type
FROM {{ source('hubspot', 'pipelines') }}
