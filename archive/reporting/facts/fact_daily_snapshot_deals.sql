WITH daily_table AS (
  {{ get_daily_table_from_snap_base(ref("base_hs_deals"), "id") }}
)

SELECT
    *
  ,  amount as amount_adjusted
FROM daily_table
