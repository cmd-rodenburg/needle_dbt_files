
WITH TICKETS AS (

    SELECT TICKET_STAGE_SK
        , TICKET_SK
        , FIRST_REPLY_DATE
        , CLOSED_DATE
        , PIPELINE_NAME
        , convert_timezone('Europe/Berlin',VALID_FROM)                          AS VALID_FROM
        , convert_timezone('Europe/Berlin',LEAST(VALID_TO, CURRENT_DATE())) 	AS VALID_TO
    FROM {{ ref('core_dim_ticket') }}
    WHERE TICKET_STAGE_NAME NOT IN ('Geschlossen','Archiviert','Warten Auf Kontakt','Warten Auf Kostal')
    AND PIPELINE_NAME IN ('Kostal', 'Support-Pipeline')


)

, CALC_TIME AS (

    SELECT
        ti.TICKET_STAGE_SK
        , ti.TICKET_SK
        --NOTE: IN THE TRANSFORMATION "-9" IS INCLUDED , AS THE TRANSFORMATION TO DATE GIVES AN HOUR OF 9 AND WE WANT IT AT 0:00
        , DATEADD(HOUR, -9, CONVERT_TIMEZONE('Europe/Berlin', cdd.DATE_KEY))                        AS DATE_KEY_WITH_TIME_ZONE
        --NOTE: KOSTAL HAS A DIFFERENT SERVICE LEVEL AGREEMENT, THUS OTHER TIMES
        , IFF(ti.PIPELINE_NAME = 'Kostal', 8,10)                                                    AS START_TIME
        , IFF(ti.PIPELINE_NAME = 'Kostal', 17,16)                                                   AS END_TIME
        --NOTE: CREATE NEW VALID FROM WHICH ARE INSIDE WORKING TIMES
        , CASE
            WHEN HOUR(ti.VALID_FROM) < START_TIME THEN DATEADD(HOUR,START_TIME - 9,ti.VALID_FROM::DATE)
            WHEN HOUR(ti.VALID_FROM) >= END_TIME THEN DATEADD(HOUR,END_TIME - 9,ti.VALID_FROM::DATE)
            ELSE VALID_FROM END                                                                     AS NEW_VALID_FROM
        , CASE
            WHEN HOUR(ti.VALID_TO) < START_TIME THEN DATEADD(HOUR,START_TIME - 9,ti.VALID_TO::DATE)
            WHEN HOUR(ti.VALID_TO) >= END_TIME THEN DATEADD(HOUR,END_TIME - 9,ti.VALID_TO::DATE)
            ELSE VALID_TO END                                                                       AS NEW_VALID_TO
        --NOTE: ADD WORKING TIMES TO LEFTOVER DAYS DAYS AND CALCULATE TIME DIFFERENCE
        , TIMEDIFF(MINUTE
            , GREATEST(NEW_VALID_FROM,DATEADD(HOUR, START_TIME,DATE_KEY_WITH_TIME_ZONE))
            , LEAST(NEW_VALID_TO, CURRENT_DATE(), DATEADD(HOUR,END_TIME,DATE_KEY_WITH_TIME_ZONE)))  AS TIME_WORKING
        , GREATEST(TIMEDIFF(MINUTE
            , GREATEST(NEW_VALID_FROM,DATEADD(HOUR, START_TIME,DATE_KEY_WITH_TIME_ZONE))
            , LEAST(NEW_VALID_TO, CURRENT_DATE(), DATEADD(HOUR,END_TIME,DATE_KEY_WITH_TIME_ZONE), FIRST_REPLY_DATE))
            ,0)                                                                                     AS TIME_FIRST_REPLY
    FROM TICKETS ti
    LEFT JOIN  {{ ref('core_dim_date') }}  cdd
        ON CDD.DATE_KEY <= LEAST(ti.VALID_TO::DATE, IFNULL(ti.CLOSED_DATE::DATE, CURRENT_DATE()))
        AND CDD.DATE_KEY >= ti.VALID_FROM::DATE
    WHERE CDD.IS_HOLIDAY = FALSE AND CDD.IS_WEEKDAY = TRUE
    HAVING (TIME_WORKING != 0 OR TIME_FIRST_REPLY IS NOT NULL)

)

SELECT TICKET_SK
    , SUM(TIME_WORKING)     /60     AS WORKING_TIME
    , SUM(TIME_FIRST_REPLY) /60     AS TIME_FIRST_REPLY
FROM CALC_TIME
GROUP BY TICKET_SK


