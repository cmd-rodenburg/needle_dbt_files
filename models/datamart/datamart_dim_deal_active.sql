/*
 * Get all current deal information from the core_dim_deal table
 */
SELECT
    {{ dbt_utils.star(
        ref('core_dim_deal'),
        except=["ACTIVE_INDICATOR", "VALID_FROM", "VALID_TO", "DEAL_STAGE_SK"]
    ) }}
FROM {{ ref('core_dim_deal') }}
WHERE ACTIVE_INDICATOR = TRUE
