WITH daily_table AS (
  {{ get_daily_table_from_snap_base(ref("base_hs_tickets"), "id") }}
)
SELECT * FROM daily_table
