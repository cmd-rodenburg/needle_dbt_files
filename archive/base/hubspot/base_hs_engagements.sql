WITH raw_data AS (

  SELECT * FROM {{ source('hubspot', 'engagements') }}

), unpack_json AS (

  SELECT
      id
    , hs_engagement_type AS type
    , hs_i_cal_uid AS uid
    --, (engagement ->> 'active') = 'true' AS is_active
    , hs_activity_type AS activity_type
    , hs_all_accessible_team_ids AS all_accessible_team_ids
    , hs_body_preview AS body_preview
    , hs_body_preview_html AS body_preview_html
    , hs_body_preview_is_truncated AS body_preview_is_truncated
    , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
    , NULLIF(hs_created_by, '')::BIGINT AS created_by
    , hs_lastmodifieddate::TIMESTAMP WITH TIME ZONE AS last_updated
    , NULLIF(hs_modified_by, '')::BIGINT AS modified_by
    , hs_gdpr_deleted AS is_gdpr_deleted
    , hs_email_tracker_key AS key
    , NULLIF(hubspot_owner_id, '')::BIGINT AS ownerid
    --, (engagement ->> 'portalId')::BIGINT AS portalid
    , hs_queue_membership_ids AS queue_membership_ids
    , hs_meeting_source AS source
    , hs_meeting_source_id AS sourceid
    , NULLIF(hubspot_team_id, '')::BIGINT AS teamid

    -- TODO: unpack as applicable
    , ewah_associations_to_deals
    --, attachments
    , hs_scheduled_tasks AS scheduled_tasks
    --, metadata

  FROM raw_data AS rd

)

select * from unpack_json
