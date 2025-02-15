{%- macro timedelta(interval, period) -%}
  EXTRACT(EPOCH FROM {{ interval }})::NUMERIC
  {%- if period.startswith("s") -%}
    {# nothing to add #}
  {%- elif period.startswith("m") -%}
    / 60
  {%- elif period.startswith("h") -%}
    / 60 / 60
  {%- elif period.startswith("d") -%}
    / 60 / 60 / 24
  {%- else -%}
    {% do exceptions.raise_compiler_error("Invalid period choice: " ~ period) %}
  {%- endif -%}
{%- endmacro -%}
