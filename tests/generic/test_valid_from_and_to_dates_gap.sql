/*
 * Tests if there are gaps inbetween the max/min valid to/from date values grouped by the column
 */

{% test valid_from_and_to_dates_gap(model, column_name) %}

WITH CTE as (

    SELECT
         {{ column_name }}                                              AS GROUP_BY_ID
        , VALID_FROM    ::DATE                                          AS VALID_FROM
        , LAG(VALID_TO) OVER (PARTITION BY GROUP_BY_ID ORDER BY VALID_TO, VALID_FROM )::DATE AS LAG_VALID_TO
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL

)

SELECT
    GROUP_BY_ID
    , VALID_FROM
    , LAG_VALID_TO
    , VALID_FROM - LAG_VALID_TO AS DAY_DIFFERENCE
FROM CTE
WHERE ABS(DAY_DIFFERENCE) > 0
	AND LAG_VALID_TO IS NOT NULL

{% endtest %}


