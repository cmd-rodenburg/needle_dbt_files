{% set associations = {
  "company": "associated_companies",
  "contact": "associated_contacts",
  "deal": "associated_deals",
  "engagement": "associated_engagements",
} %}

WITH raw AS (

  SELECT
      id
    {% for _, field in associations.items() %}, {{ field }}{% endfor %}
  FROM {{ ref("fact_tickets") }}

{% for object, field in associations.items() %}

  ), {{ object }} AS (

    SELECT raw.id AS ticket_id, '{{ object }}' AS object_type, un AS object_id
    FROM raw, UNNEST({{ field }}) AS un
    WHERE NOT {{ field }} IS NULL

{% endfor %}

), combined AS (

  {% for object, _ in associations.items() %}
    SELECT * FROM {{ object }}
    {{ "UNION ALL" if not loop.last }}
  {% endfor %}

)

SELECT * FROM combined
