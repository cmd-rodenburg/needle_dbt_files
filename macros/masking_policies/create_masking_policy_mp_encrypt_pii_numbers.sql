{% macro create_masking_policy_mp_encrypt_pii_numbers(node_database, node_schema) %}

CREATE MASKING POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.mp_encrypt_pii_numbers AS (val integer)
  RETURNS integer ->
      CASE WHEN CURRENT_ROLE() IN ('BI_DEVELOPER', 'BI_TRANSFORMER') THEN val
      ELSE 0
      END

{% endmacro %}