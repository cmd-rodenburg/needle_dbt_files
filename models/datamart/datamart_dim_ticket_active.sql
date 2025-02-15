/*
 * Get all current ticket information from the core_dim_ticket table
 */

 {{ config(
    tags=["tickets"]
) }}


SELECT
    {{ dbt_utils.star(
        ref('core_dim_ticket'),
        except=["ACTIVE_INDICATOR", "VALID_FROM", "VALID_TO"]
    ) }}
FROM {{ ref('core_dim_ticket') }}
WHERE ACTIVE_INDICATOR = TRUE
