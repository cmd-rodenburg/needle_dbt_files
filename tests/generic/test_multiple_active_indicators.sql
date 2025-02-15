/*
 * Tests whether there are multiple active records per element in staging
 */


{% test multiple_active_indicators(model, column_name) %}

WITH all_dups AS (


select {{ column_name }}
	, SUM(ACTIVE_INDICATOR::INT)
from {{ model }}
GROUP BY {{ column_name }}
HAVING  SUM(ACTIVE_INDICATOR::INT) != 1

)

SELECT {{ column_name }}
FROM all_dups


{% endtest %}