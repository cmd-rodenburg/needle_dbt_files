/*
 * Tests if there are gaps inbetween the max/min valid to/from time values grouped by the column
 * min gap = 1 day
 */

{% test valid_from_and_to_times_gap(model, column_name) %}

WITH CTE as (

    SELECT
        {{ column_name }}                                                                                      AS GROUP_BY_ID
        , VALID_FROM    ::TIMESTAMP                                                                            AS VALID_FROM
        , LAG(VALID_TO::TIMESTAMP ) OVER (PARTITION BY GROUP_BY_ID ORDER BY VALID_TO, VALID_FROM )::TIMESTAMP  AS LAG_VALID_TO
    FROM {{ model }}
    WHERE {{ column_name }} IS NOT NULL

)

SELECT
    GROUP_BY_ID
    , VALID_FROM
    , LAG_VALID_TO
    , DATEDIFF(m, VALID_FROM, LAG_VALID_TO)  AS TIME_DIFFERENCE
FROM CTE
WHERE ABS(TIME_DIFFERENCE) > 0
	AND LAG_VALID_TO IS NOT NULL

{% endtest %}


