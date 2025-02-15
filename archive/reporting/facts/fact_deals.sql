WITH raw AS (

  {{ get_current_table_from_snap_base(ref("base_hs_deals")) }}

), companies AS (

  SELECT
      id
    , associated_deals
    , created_at
  FROM {{ ref("dim_companies") }}

), contacts AS (

  SELECT
      id
    , source
    , original_source
    , original_source_1
    , original_source_2
    , latest_source
    , latest_source_1
    , latest_source_2
    , created_at
  FROM {{ ref("dim_contacts") }}

), deal_company_link AS (
  /*
   *  In Hubspot, deals and companies have a n-to-n relationship.
   *  At Solytic, we expect that if this occurs, it is due to a duplication
   *  of the company. Thus, deduplicate by matching each deal to the newest company.
   */

   WITH unnested_deals AS (
      SELECT
          co.id AS company_id
        , ad.value::BIGINT AS deal_id
        , ROW_NUMBER() OVER (PARTITION BY ad.value ORDER BY co.created_at DESC) AS _rn
      FROM companies AS co
        , JSONB_ARRAY_ELEMENTS(co.associated_deals) AS ad
   )
   SELECT company_id, deal_id FROM unnested_deals WHERE _rn = 1

), add_company_id AS (

  SELECT
      raw.*
    , dcl.company_id
  FROM raw
    LEFT JOIN deal_company_link AS dcl
      ON dcl.deal_id = raw.id

), add_first_created_contact AS (

  SELECT DISTINCT ON (raw.id)
      raw.*
    , c.id AS contact_id
    , c.source
    , c.original_source
    , c.original_source_1
    , c.original_source_2
    , c.latest_source
    , c.latest_source_1
    , c.latest_source_2
  FROM add_company_id AS raw
    , JSONB_ARRAY_ELEMENTS(raw.associated_contacts) AS jae
    LEFT JOIN contacts AS c
      ON c.id = jae.value::BIGINT
  ORDER BY raw.id, c.created_at ASC

), deal_stage_history_pivoted AS (
  /*
   * Note: the stage_entered_at macro takes the stage_id. Due to historic reasons,
   *  the names of the ids of the stages may not match how they are used in
   *  practice anymore. E.g. the "contractsent" id is now the lukewarm stage.
   *  While confusing, it is better to use the ids instead of the labels/names,
   *  because labels/names can and do change over time.
   */

  SELECT
      deal_id
    , {{ stage_entered_at("appointmentscheduled") }} cold_at
    , {{ stage_entered_at("contractsent") }} AS lukewarm_at
    , {{ stage_entered_at("2497061") }} AS warm_at
    , {{ stage_entered_at("16816984") }} AS hot_at
    , {{ stage_entered_at("80cd8162-bf4e-4ad8-9da2-963871b64e30") }} AS offer_sent_at
    , {{ stage_entered_at("a341d7fe-87cb-43a6-bd1e-c401c5d4faec") }} AS signed_at
    , {{ stage_entered_at("10474198") }} AS onboarding_at
    , {{ stage_entered_at("10474199") }} AS operation_at
    , {{ stage_entered_at("c453ff17-81ab-4a25-97d2-f4f5f5393de0") }} AS lost_at
    , {{ stage_entered_at("2417022") }} AS disqualified_at
    , {{ stage_entered_at("2417023") }} AS terminated_at
    , MIN(stage_entered_at) AS earliest_stage_change_at

  FROM {{ ref("fact_deal_stage_progression") }}
  WHERE pipeline_id = 'default'
    -- Only use stage changed after start of the historization, as the first
    -- stage change before that is incorrectly identified as the last updated_at
    -- timestamp of the deal, irrespective of whether the stage changed then or not
    AND stage_entered_at > '2021-07-23'::TIMESTAMP WITH TIME ZONE
  GROUP BY 1

), add_stage_timestamps_1 AS (

  SELECT
      raw.*
    , s1.earliest_stage_change_at
    , s1.terminated_at AS stage_terminated_at
    , s1.disqualified_at AS stage_disqualified_at
    , s1.lost_at AS stage_lost_at
    , s1.operation_at AS stage_operation_at
    , {{ deal_stager(["s1.operation_at", "s1.onboarding_at"]) }} AS stage_onboarding_at
    , {{ deal_stager(["s1.onboarding_at", "s1.operation_at", "s1.terminated_at", "s1.signed_at"]) }}
      AS stage_signed_at
  FROM add_first_created_contact AS raw
    LEFT JOIN deal_stage_history_pivoted AS s1
      ON s1.deal_id = raw.id

), add_stage_timestamps_2 AS (

  SELECT
      s1.*
    , {{ deal_stager(["s1.stage_signed_at", "s2.offer_sent_at"]) }}
      AS stage_offer_sent_at
    , {{ deal_stager(["s1.stage_signed_at", "s2.offer_sent_at", "s2.hot_at"]) }}
      AS stage_hot_at
    , {{ deal_stager(["s1.stage_signed_at", "s2.offer_sent_at", "s2.hot_at", "s2.warm_at"]) }}
      AS stage_warm_at
    , {{ deal_stager(["s1.stage_signed_at", "s2.offer_sent_at", "s2.hot_at", "s2.warm_at", "s2.lukewarm_at"]) }}
      AS stage_lukewarm_at
    , {{ deal_stager(["s1.stage_signed_at", "s2.offer_sent_at", "s2.hot_at", "s2.warm_at", "s2.lukewarm_at", "s2.cold_at"]) }}
      AS stage_cold_at

  FROM add_stage_timestamps_1 AS s1
    LEFT JOIN deal_stage_history_pivoted AS s2
      ON s2.deal_id = s1.id

), add_stage_timedeltas AS (

  SELECT
      *
    , EXTRACT(EPOCH FROM stage_warm_at - stage_lukewarm_at)::NUMERIC /60/60/24
      AS days_lukewarm_to_warm
    , EXTRACT(EPOCH FROM stage_hot_at - stage_warm_at)::NUMERIC /60/60/24
      AS days_warm_to_hot
    , EXTRACT(EPOCH FROM stage_offer_sent_at - stage_hot_at)::NUMERIC /60/60/24
      AS days_hot_to_offer_sent
    , EXTRACT(EPOCH FROM stage_signed_at - stage_offer_sent_at)::NUMERIC /60/60/24
      AS days_offer_sent_to_signed
    , EXTRACT(EPOCH FROM stage_onboarding_at - stage_signed_at)::NUMERIC /60/60/24
      AS days_signed_to_onboarding
    , EXTRACT(EPOCH FROM stage_operation_at - stage_onboarding_at)::NUMERIC /60/60/24
      AS days_onboarding_to_operation
  FROM add_stage_timestamps_2

), final AS (

  SELECT
      *
    , EXTRACT(DAYS FROM NOW() - notes_last_contacted_at) AS days_since_last_contact
  FROM add_stage_timedeltas

)

SELECT * FROM final
