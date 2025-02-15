WITH SNAP_PRODUCTS AS (

	SELECT * FROM BI_SOLYTIC.ANALYTICS_SNAPSHOTS.SNAP_HUBSPOT_PRODUCTS
	WHERE DBT_VALID_TO IS NULL

)

SELECT
	"id"							::int			AS PRODUCT_ID
	, "name"										AS PRODUCT_NAME
	, "description"									AS PRODUCT_DESCRIPTION
	, "hs_folder_id"				::int			AS FOLDER_ID
	, "price"						::numeric(8,2) 	AS UNIT_PRICE
	, "hs_cost_of_goods_sold"		::numeric(8,2) 	AS UNIT_COST
	, "hs_recurring_billing_period"					AS RECURRING_DURATION
	, "recurringbillingfrequency"					AS BILLING_FREQUENCY
FROM SNAP_PRODUCTS
