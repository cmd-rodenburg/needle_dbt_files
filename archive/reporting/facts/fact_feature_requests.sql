/*
 * Get the latest version of the fact_feature_requests table
 */

{{ select_star_except(ref("fact_daily_snapshot_company_feature_requests"), ["is_current", "snap_id", "valid_date"]) }}
WHERE is_current
