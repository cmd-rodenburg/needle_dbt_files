{{ config(
    tags=["companies"],
	materialized='table'
) }}

WITH SITES_COMBI AS (

	SELECT IFNULL(TRY_TO_NUMBER(sos.CUSTOMERID ),omc.ID) 		AS COMPANY_ID
		, TRY_TO_NUMBER(omc.COMPANYCODE)						AS HUBSPOT_COMPANY_ID
		, IFNULL(TRY_TO_NUMBER(sos.ID	),dat.DEFAULTSITEID)	AS SITE_ID
		, TRY_TO_NUMBER( DAT.ID)								AS DATA_SOURCE_ID
		, dat.STATUS
		, dat.DELETEDAT											AS DELETED_DATE
	FROM  {{ source('operation', 'datasource') }} 	dat
	LEFT JOIN {{ source('operation', 'site') }} 	sos  	ON dat.DEFAULTSITEID = sos.ID
	LEFT JOIN {{ source('operation', 'company') }} omc 	ON  sos.CUSTOMERID = omc.ID

)
SELECT
	combi.COMPANY_ID
	,combi.HUBSPOT_COMPANY_ID
	,combi.SITE_ID
	,IFNULL(combi.DATA_SOURCE_ID, DSDS.DATA_SOURCE_ID) 		AS DATA_SOURCE_ID
	,DSDS.DATA_HANDLER
	,DSDS.ENTRY_DATE										AS DATE_KEY
	,DSDS.SUM_PROCESSING_DURATION_MILLIS 					AS SUM_PROCESSING_DURATION
	,DSDS.SUM_DATA_SIZE
	,DSDS.COUNT_ENTRIES
	,combi.STATUS
	, combi.DELETED_DATE
FROM {{ source('operation_events', 'data_source_event') }}	DSDS
LEFT JOIN SITES_COMBI combi ON combi.DATA_SOURCE_ID = DSDS.DATA_SOURCE_ID