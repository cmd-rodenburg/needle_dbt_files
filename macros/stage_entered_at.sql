{%- macro stage_entered_at(stage_id) -%}
  {#
   # Small helper macro used in fact_deals. Used to calculate the first time a deal
   # entered a particular stage.
   #}
  MIN(CASE
    WHEN stage_id = '{{ stage_id }}'
    THEN stage_entered_at
  END)
{%- endmacro -%}

{%- macro deal_stager(timestamp_list) -%}
  {#
   # Use exclusively in fact_deals to calculate stage change dates.
   #
   # The core issue is that there are deals that have been created before
   # the start of the historization. These do not have values for the early
   # stages of the deal, only for those after the historization started.
   # At the same time, we use `LEAST` to coalesce the value of an early
   # stage to that of a later stage in case a stage was skipped, to have
   # continuity in stage timestamps. E.g. if a deal went from cold to warm
   # without going to lukewarm, then lukewarm_at should be the same as warm_at
   # instead of being `NULL`. In combination, these two facts prove hard to
   # reconcile. This macro contains the logic of a reconciliation attempt.
   #
   #}

  CASE
    WHEN created_at >= '2021-07-23' THEN
      {# New deals: consider the stages normally #}
      LEAST(
        {%- for item in timestamp_list -%}
          {{ item }}{{ "," if not loop.last }}
        {%- endfor -%}
      )
    ELSE
      {#
       # Old deals: early stages ought to be NULL when they in reality would
       # have been before the start of the historization.
       #}
      COALESCE(
        NULLIF(LEAST(
          {%- for item in timestamp_list -%}
            {{ item }}{{ "," if not loop.last }}
          {%- endfor -%}
        ), s1.earliest_stage_change_at)
        , CASE
            WHEN {{ timestamp_list[-1] }} = s1.earliest_stage_change_at
              {# this stage is the first change after start of historization #}
              THEN {{ timestamp_list[-1] }}
            ELSE NULL {# if there was a change in this stage, it was after the start
                        of the historization #}
          END
      )
  END

{%- endmacro -%}
