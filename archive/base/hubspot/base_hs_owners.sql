WITH snap_data AS (SELECT * FROM {{ ref("snap_hubspot_owners") }})
, source_data AS (SELECT * FROM {{ source("hubspot", "owners") }})
, last_engagement_by_user AS (

  SELECT
      created_by
    , MAX(created_at) AS latest_engagement
  FROM {{ ref("base_hs_engagements") }}
  GROUP BY 1

)
, transform AS (

  SELECT
      id::BIGINT
    , dbt_valid_from::TIMESTAMP WITH TIME ZONE AS valid_from
    , dbt_valid_to::TIMESTAMP WITH TIME ZONE AS valid_to
    , dbt_valid_to IS NULL AS is_current
    , email
    , "firstName" AS first_name
    , "lastName" AS last_name
    , "firstName" || ' ' || "lastName" AS full_name
    , "userId" AS user_id
    , "createdAt"::TIMESTAMP WITH TIME ZONE AS created_at
    , "updatedAt"::TIMESTAMP WITH TIME ZONE AS updated_at
    , teams

  FROM snap_data

)
, add_is_active AS (

  SELECT
      t.*
    /*
     * Deactivated owners disappear from the source data -> the last (!) snapshot
     * will never change.
     * The latest snapshot may be is_active=FALSE if the id does not
     * exist in the source data anymore.
     * Further, if the owner exists, check whether there was any activity in the last
     * 30 days, as measured from the valid_to. For the latest snapshot, check against
     * the current date.
     */
    , CASE
        -- current snapshot: if doesn't exist in the data, then is not active
        WHEN t.valid_to IS NULL AND NOT t.id IN (SELECT id::BIGINT FROM source_data)
          THEN FALSE
        -- any snapshot: if the last interaction is 30 days before the valid_from, false
        WHEN COALESCE(t.valid_to, NOW()) > (INTERVAL '30 day' + le.latest_engagement)
          THEN FALSE
        ELSE TRUE
      END AS is_active
  FROM transform AS t
    LEFT JOIN last_engagement_by_user AS le
      ON le.created_by = t.user_id

)

SELECT * FROM add_is_active
