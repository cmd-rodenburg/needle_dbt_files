WITH raw AS (
  {{ get_current_table_from_snap_base(ref("base_hs_companies")) }}
)
SELECT id, associated_deals, associated_contacts
FROM raw
WHERE lower(name) LIKE '%solytic%'
