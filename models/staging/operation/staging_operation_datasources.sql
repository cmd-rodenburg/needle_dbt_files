{{ config(materialized='table') }}

-- NOTE: variable for datasource type
{% set device_type = dbt_utils.get_column_values(table = source('operation', 'device'), column = "TYPE") %}

WITH DATA_SOURCE_LAST_INGESTIONS AS (
	SELECT DATA_SOURCE_ID
		, NULLIF(listagg(DISTINCT DATA_HANDLER, ';') WITHIN GROUP (ORDER BY DATA_HANDLER) , '')	AS DATA_HANDLER_TYPE
		, MAX(ENTRY_DATE) AS LAST_INGESTION
	FROM {{ source('operation_events', 'data_source_event') }}
	GROUP BY DATA_SOURCE_ID
)


SELECT dev.DATASOURCEID																				AS DATA_SOURCE_ID
	, dat.DEFAULTSITEID  																			AS SITE_ID
	, COUNT(*)																						AS DEVICES
	, dst.DATA_HANDLER_TYPE
	, IFF(MAX(dst.LAST_INGESTION) IS NULL, 'Registered',
		IFF(DATEDIFF(day, MAX(dst.LAST_INGESTION), CURRENT_DATE()) <=31, 'Active', 'Historical'))	AS DATASOURCE_STATUS
	-- NOTE: This compiled code will count the number per datasource types
	, {%- for device_type in device_type -%}
	SUM(IFF(TYPE =  '{{device_type}}'	, 1, 0)) 													AS c_{{device_type}}
	,		{% endfor %}
	 max(IFF(dm.value = 'KOSTAL Smart Energy Meter', TRUE, FALSE))									AS SMART_ENERGY_METER
	, NULLIF(listagg(DISTINCT dev.TYPE, ';') WITHIN GROUP (ORDER BY dev.TYPE) , '')					AS DEVICE_TYPES
	, IFF(YEAR(dat.CREATEDAT::DATE) < 2006, '2006-01-01', dat.CREATEDAT ::DATE) 					AS CREATE_DATE
	, MAX(dst.LAST_INGESTION)																		AS LAST_INGESTION
FROM  {{ source('operation', 'device') }} dev
LEFT JOIN {{ source('operation', 'datasource') }} dat ON DAT.ID = dev.DATASOURCEID
LEFT JOIN {{ source('operation', 'datasource_meta') }} dm ON dm.DATASOURCEID = dat.ID AND dm.value = 'KOSTAL Smart Energy Meter'
LEFT JOIN DATA_SOURCE_LAST_INGESTIONS dst ON dst.DATA_SOURCE_ID = dev.DATASOURCEID
WHERE DEV.PHYSICALDEVICEID IS NULL
	AND lower(dat.STATUS) <> 'inactive'
	AND dat.DELETEDAT IS NULL
	AND dat.PHYSICALDATASOURCEID IS NULL
GROUP BY DEV.DATASOURCEID
	, dst.DATA_HANDLER_TYPE
	, dat.DEFAULTSITEID
	, dat.STATUS
	, dat.CREATEDAT
	, dat.LASTUPDATEDAT
