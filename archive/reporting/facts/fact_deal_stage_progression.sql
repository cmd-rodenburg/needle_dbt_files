WITH raw AS (

  SELECT * FROM {{ ref("hs_dealstage_history") }}

), stages AS (

  SELECT * FROM {{ ref("dim_pipeline_stages") }}

), get_first_new_stage_event AS (

  SELECT DISTINCT ON (deal_id, new_stage_id)
      deal_id
    , new_stage_id AS stage_id
    , valid_from AS stage_entered_at
  FROM raw
  ORDER BY 1,2,3 ASC

), add_pipeline_id AS (

  SELECT
      e.*
    , COALESCE(st.pipeline_id, '(deleted stage or pipeline)') AS pipeline_id
    , COALESCE(st.stage_name, '(deleted stage or pipeline)') AS stage_name
  FROM get_first_new_stage_event AS e
    LEFT JOIN stages AS st
      ON st.id = e.stage_id

)

SELECT * FROM add_pipeline_id
