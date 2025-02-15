{% macro create_masking_policy_mp_encrypt_pii(node_database, node_schema) %}

CREATE MASKING POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.mp_encrypt_pii AS (val string)
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('BI_DEVELOPER', 'BI_TRANSFORMER') THEN val
      ELSE '**********'
      END

{% endmacro %}