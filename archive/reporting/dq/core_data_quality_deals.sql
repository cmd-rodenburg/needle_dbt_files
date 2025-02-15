WITH DEAL_ALL AS (

	SELECT * FROM {{ ref("staging_hubspot_deals") }}
	WHERE ACTIVE_INDICATOR = TRUE

)


SELECT DEAL_ID                                          AS "deal_id"
	, OFFER_EXPIRATION_DATE                             AS "offer_expiration_date"                            
	, EXPECTED_DEAL_CLOSING_MONTH                       AS "expected_deal_closing_month"                                    
	, DEAL_STAGE_ID                                     AS "deal_stage_id"                    
	, PIPELINE_ID                                       AS "pipeline_id"                    
	, OWNER_ID                                          AS "owner_id"                
	, COMPANY_ID                                        AS "company_id"                    
	, HUBSPOT_TEAM_ID                                   AS "hubspot_team_id"                        
	, NUM_LINE_ITEMS                                    AS "num_line_items"                        
	, CONTRACT_START_DATE                               AS "contract_start_date"                            
	, CONTRACT_END_DATE                                 AS "contract_end_date"                        
	, CURRENT_CONTRACT_END                              AS "current_contract_end"                            
	, CONTRACT_DURATION                                 AS "contract_duration"                        
	, CLIENT_MOMENTUM                                   AS "client_momentum"                        
	, WHITELABEL_INDICATOR                              AS "whitelabel_indicator"                            
	, COMPANY_MWP                                       AS "company_mwp"                    
	, COMPANY_MWP_CATEGORY                              AS "company_mwp_category"                            
	, DEAL_MWP                                          AS "deal_mwp"                
	, PLANT_KWP                                         AS "plant_kwp"                
	, PLANTS_TOTAL                                      AS "plants_total"                    
	, AMOUNT                                            AS "amount"                
	, AMOUNT_IN_HOME_CURRENCY                           AS "amount_in_home_currency"                                
	, PAYMENT_TYPE                                      AS "payment_type"                    
	, DEAL_ORIGINAL_SOURCE_TYPE                         AS "deal_original_source_type"                                
FROM DEAL_ALL AS DA
