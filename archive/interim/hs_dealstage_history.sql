WITH historization AS (
  {{ create_change_history("dealstage", ref('snap_hubspot_deals')) }}
)

SELECT
    id::BIGINT AS deal_id
  , field_value AS new_stage_id
  , previous_field_value AS old_stage_id
  , valid_from
  , valid_until
FROM historization
