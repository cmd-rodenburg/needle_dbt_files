
SELECT soa.DATE_KEY
  	, soa.OPERATIONAL_USER_ID
	, shc.COMPANY_SK	
	, soa.COUNT_QUERIES
FROM {{ ref('staging_operation_api_tracking') }} soa
LEFT JOIN {{ ref('core_dim_company') }} shc USING(OPERATIONAL_ID)