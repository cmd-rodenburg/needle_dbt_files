{{ config(
    tags=["companies"]
) }}


with RAW_COMPANY_DATA as (

	select * from "BI_SOLYTIC"."ANALYTICS_SNAPSHOTS".snap_hubspot_companies
	WHERE "id" IN (SELECT "id" FROM {{ source('hubspot', 'companies') }} )
		AND DBT_VALID_TO IS NULL

)

SELECT
	"id"::bigint															AS COMPANY_ID
	, "name"																AS COMPANY_NAME
	, IFF(LEFT("reviso_customer_id", 2) ='10'
		, RIGHT("reviso_customer_id", LEN("reviso_customer_id") - 2)
		, "reviso_customer_id") 						:: INT				AS SEVDESK_CUSTOMER_ID
	, "country"																AS COUNTRY
	, INITCAP("industry")													AS INDUSTRY
	, NULLIF("tarr", '')			::numeric(10,2)							AS TARR
	, NULLIF("segment__revenue",'')											AS SEGMENT_REVENUE
	, NULLIF("segment__potential",'')										AS SEGMENT_POTENTIAL
	, IFF("current_customer" IS NULL, FALSE, TRUE)							AS CURRENT_CUSTOMER
	, INITCAP("lifecyclestage")												AS LIFECYCLESTAGE
	, "mwp__geplant"			 											AS MWP_PLANNED
	, NULLIF("mwp__aktuell", '')	::numeric(10,5) 						AS MWP_CURRENT
	, "mwp__signed___total"	 												AS MWP_SIGNED_TOTAL
	, "n2_0_anlagen__created" 		::INT									AS SITES_2_CREATED
	, NULLIF("portal__used",'') 											AS PORTAL_USED
	, NULLIF("potential_in_mwp",'')	::numeric(10,5)							AS POTENTIAL_IN_MWP
	, "hubspot_owner_id"													AS HUBSPOT_OWNER_ID
	, "num_associated_deals"												AS NUM_DEALS
	, "must_have_features"													AS MUST_HAVE_FEATURE
	, IFF("nice_to_have_features" IS NOT NULL
		AND "nice_to_have_other_features" IS NOT NULL,
		CONCAT("nice_to_have_features", ';', "nice_to_have_other_features"),
		IFNULL("nice_to_have_features", "nice_to_have_other_features")) 	AS NICE_TO_HAVE_FEATURE
FROM RAW_COMPANY_DATA