{% macro create_masking_policy_mp_encrypt_pii_customer_success(node_database, node_schema) %}

CREATE MASKING POLICY IF NOT EXISTS {{node_database}}.{{node_schema}}.mp_encrypt_pii_customer_success AS (val string)
  RETURNS string ->
      CASE WHEN CURRENT_ROLE() IN ('BI_DEVELOPER', 'BI_TRANSFORMER', 'CUSTOMER_SUCCESS_PII') THEN val
      ELSE '**********'
      END

{% endmacro %}