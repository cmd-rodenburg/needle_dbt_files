WITH portals AS (

	select
		'Solytic 2.0' 															AS PORTAL
		, IFNULL(CDC.COMPANY_SK, -1)											AS COMPANY_SK
		, sos.SITE_ID
		, sos.SITE_NAME
		, ROUND(IFF(sos.CAPACITY > 10000, sos.CAPACITY/1000, sos.CAPACITY), 2)	AS CAPACITY
		, sos.LATITUDE_															AS LATITUDE
		, sos.LONGITUDE_														AS LONGITUDE
		, sos.COUNTRY_ID
		, sos.LAST_CONTACT
		, IFNULL(sos.INSTALLATION_DATE, '2006-01-01')							AS INSTALLATION_DATE
		, sos.CREATE_DATE
		, SUM(dat.DEVICES )														AS NUMBER_OF_DEVICES
		, sos.HISTORICAL_SITE
		, sos.LIVE_SITE
		, BOOLOR_AGG(dat.SMART_ENERGY_METER)									AS SMART_METER_INDICATOR
		, SUM(dat.C_BATTERY)													AS COUNT_BATTERY
		, SUM(dat.C_SATELLITE)													AS COUNT_SATELLITE
		, NULLIF(listagg(DISTINCT dat.DATA_HANDLER_TYPE, ';') WITHIN GROUP (ORDER BY dat.DATA_HANDLER_TYPE) , '')	AS DATA_HANDLER_TYPE
		, NULLIF(listagg(DISTINCT dat.DEVICE_TYPES, ';') WITHIN GROUP (ORDER BY dat.DEVICE_TYPES) , '')	AS DEVICE_TYPE
		, sos.INSTALLATION_TYPE
		, sos.MOUNTINGTYPE														AS MOUNTING_TYPE
	from {{ ref('staging_operation_sites') }} sos
	LEFT JOIN {{ ref('staging_operation_datasources') }} dat ON sos.SITE_ID = dat.SITE_ID
	LEFT JOIN {{ ref('core_dim_company') }} cdc ON cdc.OPERATIONAL_ID = sos.COMPANY_ID
	GROUP BY CDC.COMPANY_NAME
		, IFNULL(CDC.COMPANY_SK, -1)
		, sos.SITE_ID
		, sos.SITE_NAME
		, sos.CAPACITY
		, sos.LATITUDE_
		, sos.LONGITUDE_
		, sos.COUNTRY_ID
		, sos.LAST_CONTACT
		, sos.INSTALLATION_DATE
		, sos.CREATE_DATE
		, sos.HISTORICAL_SITE
		, sos.LIVE_SITE
		, sos.INSTALLATION_TYPE
		, sos.MOUNTINGTYPE

	union all

	select
		'Solytic 1.0'													AS PORTALDATEDIFF
		, IFNULL(cdc.COMPANY_SK, -1)									AS COMPANY_SK
		, s1.SITE_ID
		, s1.SITE_NAME
		, ROUND(IFF(CAPACITY > 10000, CAPACITY/1000, CAPACITY), 2)		AS CAPACITY
		, LATITUDE
		, LONGITUDE
		, IFNULL(s1.COUNTRY_ID	, -1)									AS COUNTRY_ID
		, s1.LAST_CONTACT ::DATE										AS LAST_CONTACT
		, '2006-01-01'													AS INSTALLATION_DATE
		, '2006-01-01'													AS CREATE_DATE
		, s1.NUMBER_OF_DEVICES
		, s1.HISTORICAL_SITE
		, s1.LIVE_SITE
		, NULL 															AS SMART_METER_INDICATOR
		, NULL 															AS COUNT_BATTERY
		, NULL															AS COUNT_SATELLITE
		, NULL															AS DATA_HANDLER_TYPE
		, NULL 															AS DEVICE_TYPE
		, NULL 															AS INSTALLATION_TYPE
		, NULL															AS MOUNTING_TYPE
	from {{ ref('staging_flatfiles_portals_solytic_1') }} s1
	left join {{ ref('core_dim_company') }} cdc ON s1.COMPANY_NAME = cdc.COMPANY_NAME

	union all

	select 'Suntrol'			 										AS PORTAL
		, -1															AS COMPANY_SK
		, sun.SITE_ID
		, sun.SITE_NAME
		, ROUND(IFF(sun.CAPACITY > 10000, sun.CAPACITY/1000, sun.CAPACITY), 2)	AS CAPACITY
		, sun.LATITUDE
		, sun.LONGITUDE
		, IFNULL(sun.COUNTRY_ID, -1)									AS COUNTRY_ID
		, sun.LAST_CONTACT ::DATE										AS LAST_CONTACT
		, '2006-01-01'													AS INSTALLATION_DATE
		, '2006-01-01'													AS CREATE_DATE
		, sun.NUMBER_OF_DEVICES
		, sun.HISTORICAL_SITE
		, sun.LIVE_SITE
		, NULL 															AS SMART_METER_INDICATOR
		, NULL 															AS COUNT_BATTERY
		, NULL															AS COUNT_SATELLITE
		, NULL															AS DATA_HANDLER_TYPE
		, NULL 															AS DEVICE_TYPE
		, NULL 															AS INSTALLATION_TYPE
		, NULL															AS MOUNTING_TYPE
	from {{ ref('staging_flatfiles_portals_suntrol') }} sun

)

select ROW_NUMBER() OVER (ORDER BY PORTAL, SITE_ID) AS SITE_SK
	, PORTAL
	, COMPANY_SK
	, SITE_ID
	, SITE_NAME
	, CAPACITY
	, LATITUDE
	, LONGITUDE
	, IFNULL(COUNTRY_SK, -1)						AS COUNTRY_SK
	, LAST_CONTACT
	, INSTALLATION_DATE
	, CREATE_DATE
	, NUMBER_OF_DEVICES
	, HISTORICAL_SITE
	, LIVE_SITE
	, SMART_METER_INDICATOR
	, COUNT_BATTERY
	, COUNT_SATELLITE
	, DATA_HANDLER_TYPE
	, DEVICE_TYPE
	, INSTALLATION_TYPE
	, MOUNTING_TYPE
FROM portals
LEFT JOIN {{ ref('core_dim_country') }} USING (COUNTRY_ID)