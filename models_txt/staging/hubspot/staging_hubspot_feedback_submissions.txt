SELECT
    "id" 					        ::NUMERIC 					AS FEEDBACK_ID
    , "hs_submission_timestamp"		::TIMESTAMP WITH TIME ZONE 	AS DATE_KEY
    , "hs_contact_id"				::NUMERIC					AS CONTACT_ID
    , "hs_survey_id"				::NUMERIC(16)				AS SURVEY_ID
    , CASE
    	WHEN "hs_survey_type" = 'KNOWLEDGE'					THEN 'Frequently asked questions'
    	WHEN "hs_survey_type" = 'CUSTOM' AND SURVEY_ID = 13 THEN 'Net promoter score'
        WHEN "hs_survey_type" = 'CES'                       THEN 'Customer effort score'
    	ELSE "hs_survey_type" END 	::VARCHAR(32)				AS SURVEY_TYPE
    , "hs_ticket_id"				::NUMERIC					AS TICKET_ID
    , "hs_knowledge_article_id"		::NUMERIC					AS FAQ_ID
    , "hs_value"					::NUMERIC(4)				AS FEEDBACK_RESULT
    , "hs_sentiment"				::VARCHAR(16)				AS FEEDBACK_SENTIMENT
    , "hs_content"					::VARCHAR					AS FEEBACK_CONTENT
FROM BI_RAW.RAW_HUBSPOT."feedback_submissions"


