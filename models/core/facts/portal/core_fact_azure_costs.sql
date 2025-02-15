SELECT MONTH_FIRST_DAY																		AS DATE_KEY
    , SUM(IFF(SERVICE_FAMILY IN ('Storage', 'Databases'),COST_IN_EUR, 0 )) 					AS COSTS_DATA_STORAGE
    , SUM(IFF(SERVICE_FAMILY IN ('Compute', 'Analytics', 'Networking'), COST_IN_EUR , 0)) 	AS COSTS_PROCESSING
FROM {{ ref('staging_flatfiles_azure_costs') }} sfac
LEFT JOIN {{ ref('core_dim_date') }} dd USING (DATE_KEY)
WHERE SUBSCRIPTION_NAME != 'business analytics'
    AND SERVICE_FAMILY IN ('Storage', 'Databases','Compute', 'Analytics', 'Networking')
GROUP BY MONTH_FIRST_DAY