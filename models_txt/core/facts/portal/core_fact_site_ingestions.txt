-- NOTE: Sites can have multiple data handlers
SELECT IFNULL(cdc.COMPANY_SK, -1)	 				AS COMPANY_SK
	, cps.SITE_SK
	, de.DATA_HANDLER
	, de.DATE_KEY
	, COUNT(DISTINCT de.DATA_SOURCE_ID)				AS DATASOURCES
	, DIV0(SUM(de.SUM_PROCESSING_DURATION), 1000)	AS PROCESSING_DURATION_SEC
	, DIV0(SUM(de.SUM_DATA_SIZE), POWER(10, 6)) 	AS DATA_SIZE_GB
	, SUM(de.COUNT_ENTRIES)							AS ENTRIES
FROM {{ ref('staging_operation_datasources_events') }} 	de
LEFT JOIN {{ ref('core_dim_company') }} 				cdc ON de.COMPANY_ID = cdc.OPERATIONAL_ID
LEFT JOIN {{ ref('core_dim_site') }} 					cps ON CPS.SITE_ID = de.Site_ID AND CPS.PORTAL = 'Solytic 2.0'
GROUP BY cdc.COMPANY_SK
	, cps.SITE_SK
	, HUBSPOT_COMPANY_ID
	, de.DATA_HANDLER
	, DATE_KEY

