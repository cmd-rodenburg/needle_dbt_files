WITH snap_tickets AS (

  SELECT * FROM {{ ref("snap_hubspot_tickets") }}
  -- ignore deleted tickets
  WHERE id IN (SELECT id FROM {{ source("hubspot", "tickets") }})

), transforms AS (

  SELECT
      id::BIGINT
    , dbt_valid_from::TIMESTAMP WITH TIME ZONE AS valid_from
    , dbt_valid_to::TIMESTAMP WITH TIME ZONE AS valid_to
    , dbt_valid_to IS NULL AS is_current
    , archived AS is_archived

    , (
        SELECT ARRAY_AGG((jae->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_companies) AS jae
      ) AS associated_companies
    , (
        SELECT ARRAY_AGG((jae->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_contacts) AS jae
      ) AS associated_contacts
    , (
        SELECT ARRAY_AGG((jae->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_deals) AS jae
      ) AS associated_deals
    , (
        SELECT ARRAY_AGG((jae->>'id')::BIGINT)
        FROM JSONB_ARRAY_ELEMENTS(ewah_associations_to_engagement) AS jae
      ) AS associated_engagements

    , COALESCE(JSONB_ARRAY_LENGTH(ewah_associations_to_companies), 0)
      AS count_associated_companies
    , COALESCE(JSONB_ARRAY_LENGTH(ewah_associations_to_contacts), 0)
      AS count_associated_contacts
    , COALESCE(JSONB_ARRAY_LENGTH(ewah_associations_to_deals), 0)
      AS count_associated_deals
    , COALESCE(JSONB_ARRAY_LENGTH(ewah_associations_to_engagement), 0)
      AS count_associated_engagements

    , NULLIF(hs_num_times_contacted, '')::BIGINT AS count_times_contacted
    , NULLIF(num_notes, '')::BIGINT AS count_notes
    , NULLIF(num_contacted_notes, '')::BIGINT AS count_contacted_notes

    , hs_pipeline AS pipeline_id
    , hs_pipeline_stage AS pipeline_stage_id
    , hs_ticket_priority AS ticket_priority
    , original_priority
    , NULLIF(hubspot_owner_id, '')::BIGINT AS owner_id
    , NULLIF(hubspot_team_id, '')::BIGINT AS team_id
    , CASE
        WHEN JSONB_ARRAY_LENGTH(ewah_associations_to_companies) = 1
          THEN ewah_associations_to_companies -> 0 ->> 'id'
      END::BIGINT AS company_id
    , CASE
        WHEN JSONB_ARRAY_LENGTH(ewah_associations_to_contacts) = 1
          THEN ewah_associations_to_companies -> 0 ->> 'id'
      END::BIGINT AS contact_id
    , CASE
        WHEN JSONB_ARRAY_LENGTH(ewah_associations_to_companies) = 1
          THEN ewah_associations_to_deals -> 0 ->> 'id'
      END::BIGINT AS deal_id
    , source_type
    , subject
    , content
    , hs_resolution AS resolution
    , hs_ticket_category AS ticket_category
    , portal
    , product
    , product_type
    , service_katalog AS service_catalogue
    , NULLIF(solution_time, '')::NUMERIC AS solution_time

    -- JIRA
    , si_jira_issue_assignee
    , si_jira_issue_id
    , si_jira_issue_key
    , si_jira_issue_link
    , si_jira_issue_priority
    , si_jira_issue_reporter
    , si_jira_issue_status
    , si_jira_issue_summary
    , si_jira_sync_notes

    /* TODO: Check if this data is reliable
    , hs_time_in_1
    , hs_time_in_2
    , hs_time_in_3
    , hs_time_in_4
    , hs_time_in_5
    , hs_time_in_8502316
    */
    , tags
    , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
    , NULLIF(closed_date, '')::TIMESTAMP WITH TIME ZONE AS closed_at
    , NULLIF(hubspot_owner_assigneddate, '')::TIMESTAMP WITH TIME ZONE
      AS owner_assigned_at
    , NULLIF(first_agent_reply_date, '')::TIMESTAMP WITH TIME ZONE
      AS first_agent_replied_at
    , NULLIF(last_engagement_date, '')::TIMESTAMP WITH TIME ZONE AS last_engaged_at
    , "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at
  FROM snap_tickets

), add_timedeltas AS (

  SELECT
      *
    , {{ timedelta("closed_at - created_at", "hours") }}
      AS hours_created_to_closed
    , {{ timedelta("owner_assigned_at - created_at", "hours") }}
      AS hours_created_to_assignment
    , {{ timedelta("first_agent_replied_at - created_at", "hours") }}
      AS hours_created_to_first_reply
    , {{ timedelta("NOW() - last_engaged_at", "hours") }}
      AS hours_since_last_engagement
    , {{ timedelta("first_agent_replied_at - owner_assigned_at", "hours") }}
      AS hours_assigment_to_first_reply
    , {{ timedelta("closed_at - owner_assigned_at", "hours") }}
      AS hours_assignment_to_closed
    , {{ timedelta("closed_at - first_agent_replied_at", "hours") }}
      AS hours_first_reply_to_closed

    -- add all timestamps as dates, too
    , created_at::DATE AS created_on
    , closed_at::DATE AS closed_on
    , owner_assigned_at::DATE AS owner_assigned_on
    , first_agent_replied_at::DATE AS first_agent_replied_on
    , last_engaged_at::DATE AS last_engaged_on
    , updated_at::DATE AS updated_on
  FROM transforms

)

SELECT * FROM add_timedeltas
