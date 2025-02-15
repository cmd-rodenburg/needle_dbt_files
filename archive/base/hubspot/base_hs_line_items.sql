SELECT
	line_item.id 												as line_item_id
	, b_d.id									::bigint 		as deal_id
	--,s_li.id
	--,s_li.hs_object_id
	, s_li.hs_product_id						::bigint 		as product_id
	, s_li.hs_created_by_user_id								as user_id_created
	, s_li.hs_updated_by_user_id								as user_id_updated
	, s_li.dbt_valid_from						::timestamp
	, s_li.dbt_valid_to							::timestamp
	, case when dbt_valid_to is null then 1 else 0 end::boolean as dbt_active_ind
--	, s_li.createdate											as date_created
--	, s_li.hs_lastmodifieddate									as date_updated
	, s_li.name 												as line_item_name
	, s_li.description
	--,s_li."createdAt"
	--,s_li."updatedAt"
	, s_li.archived								::boolean
	, s_li.quantity								::int			as quantity_units
	, NULLIF(s_li.price					, '')	::numeric(10,2) as unit_price
	, NULLIF(s_li.hs_pre_discount_amount, '')	::numeric(10,2)	as pre_discount_amount
	, NULLIF(s_li.discount				, '')	::numeric(10,2)	as discount
	, NULLIF(s_li.hs_total_discount		, '')	::numeric(10,2) as total_discount
	, NULLIF(s_li.hs_discount_percentage, '')	::numeric(10,2) as discount_percentage
	, NULLIF(s_li.amount				, '')	::numeric(10,2)	as amount
	, s_li.recurringbillingfrequency
	, s_li.hs_recurring_billing_start_date						as recurring_billing_start_date
	, s_li.hs_recurring_billing_end_date						as recurring_billing_end_date
	, s_li.hs_recurring_billing_period							as recurring_billing_period
	, s_li.hs_term_in_months					::int			as term_in_months
	, s_li.hs_acv								::numeric(10,2) as acv
	, s_li.hs_arr								::numeric(10,2) as arr
	, NULLIF(s_li.hs_cost_of_goods_sold, '')	::numeric  		as cost_of_goods_sold
	, NULLIF(s_li.hs_margin		, '')			::numeric(10,2) as margin
	, NULLIF(s_li.hs_margin_acv	, '')			::numeric(10,2) as margin_acv
	, NULLIF(s_li.hs_margin_arr	, '')			::numeric(10,2) as margin_arr
	, NULLIF(s_li.hs_margin_mrr	, '')			::numeric(10,2) as margin_mrr
	, NULLIF(s_li.hs_margin_tcv	, '')			::numeric(10,2) as margin_tcv
	, NULLIF(s_li.hs_mrr		, '')			::numeric(10,2) as mrr
	, s_li.hs_position_on_quote					::int			as position_on_quote
	, s_li.hs_tcv								::numeric(10,2)	as tcv
	, s_li.hs_images							   				as images
	, s_li.hs_sku								  				as sku
	, s_li.hs_url								  				as url
-- The relationship between deals and line items is located in a JSONB column
FROM {{ source("hubspot", "deals") }} as b_d
	,lateral jsonb_to_recordset(b_d.ewah_associations_to_line_items) AS line_item(id BIGINT, type TEXT)
left join {{ ref('snap_hubspot_line_items') }} s_li on line_item.id = cast(s_li.id AS BIGINT)
