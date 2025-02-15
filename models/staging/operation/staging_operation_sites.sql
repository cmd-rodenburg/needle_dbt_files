SELECT DISTINCT
	mdl.ID 																			AS SITE_ID
	, mdl.NAME																		AS SITE_NAME
	, mdl.CUSTOMERID 																AS COMPANY_ID
	, mdl.OWNERID																	AS OWNER_ID
	, mdl.INSTALLEDCAPACITY	 														AS CAPACITY
	, mdl.INSTALLATIONTYPE															AS INSTALLATION_TYPE
	, mdl.MOUNTINGTYPE
	, mdl.ISPUBLIC
	, IFF(ad.LATITUDE between 1 and -1, NULL, ad.LATITUDE)							AS LATITUDE_
	, IFF(ad.LONGITUDE between 1 and -1, NULL, ad.LONGITUDE)						AS LONGITUDE_
	, TO_GEOGRAPHY(CONCAT('POINT(', LONGITUDE_,' ', LATITUDE_, ')' ))				AS COORDINATES
	, CASE lower(trim(ad.COUNTRY))
			WHEN 'united kingdom' 	THEN 826
			WHEN 'south korea' 		THEN 410
			WHEN 'espanya' 			THEN 724
			WHEN 'united states' 	THEN 840
			WHEN 'vereinigte staaten von amerika'THEN 840
			WHEN 'czech republic' 	THEN 203
			WHEN 'åland' 				THEN 246
			WHEN 'bolivia, plurinational state of' 				THEN 68
			WHEN 'royaume du maroc' 	THEN 504
			WHEN 'congo (rdc)' 		THEN 180
			WHEN 'hrvatska' 		THEN 191
			WHEN 'hrvaška' 		THEN 191
			WHEN 'eesti' 			THEN 233
			WHEN 'ellás' 		THEN 300
			WHEN 'macedonia, the former yugoslav republic of' 		THEN 807
	END			 																	AS COUNTRY_
	, COALESCE(FC1.COUNTRY_ID, FC2.COUNTRY_ID, COUNTRY_, -1 )						AS COUNTRY_ID
	, IFF(YEAR(mdl.INSTALLATIONDATE) < 2006, '2006-01-01', mdl.INSTALLATIONDATE) 	AS INSTALLATION_DATE
	, IFF(YEAR(mdl.CREATEDAT::DATE) < 2006, '2006-01-01', mdl.CREATEDAT::DATE)		AS CREATE_DATE
	, met.LASTCONTACT::TIMESTAMP 													AS LAST_CONTACT
	, IFF(met.LASTCONTACT IS NULL, FALSE, TRUE) 	 			 					AS HISTORICAL_SITE
	, IFNULL(DATEDIFF(day, met.LASTCONTACT::DATE, CURRENT_DATE) <=31, FALSE)		AS LIVE_SITE
FROM {{ source('operation', 'site') }} AS mdl
LEFT JOIN {{ source('operation_metrics', 'site') }} AS met ON mdl.id = met.SITEID
LEFT JOIN {{ source('operation', 'address') }} AS ad ON ad.ID = mdl.ADDRESSID
LEFT JOIN {{ ref('staging_flatfiles_countries') }} FC1 ON FC1."LANGUAGE" = 'en' AND st_contains(FC1.COUNTRY_GEOGRAPHY , coordinates)
LEFT JOIN {{ ref('staging_flatfiles_countries') }} FC2 ON lower(FC2.COUNTRY_NAME_LOCAL) = lower(trim(ad.COUNTRY))
WHERE mdl.TYPE <> 'Subsite'