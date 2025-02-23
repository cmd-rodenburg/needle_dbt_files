
WITH invoices AS (

	SELECT rev.DATE_KEY
		, rev.DEAL_SK
		, rev.COMPANY_SK
		, DATE_TRUNC(month, rev.INVOICE_PERIOD_FROM)::DATE										AS PERIOD_FROM
		, LAST_DAY(rev.INVOICE_PERIOD_UNTIL)::DATE 												AS PERIOD_TO
		, DENSE_RANK() OVER (PARTITION BY rev.DEAL_SK, rev.COMPANY_SK ORDER BY PERIOD_FROM)		AS RN_
		, DENSE_RANK() OVER (PARTITION BY rev.DEAL_SK, rev.COMPANY_SK ORDER BY PERIOD_TO DESC) 	AS RN_DESC_
		, IFF(RN_ = 1, IFNULL(rev.CONTRACT_START_DATE, PERIOD_FROM), PERIOD_FROM) 				AS CLEAN_FROM
		, IFF(RN_DESC_ = 1 AND rev.CONTRACT_END_DATE > PERIOD_TO,
			IFNULL(rev.CONTRACT_END_DATE, PERIOD_TO), PERIOD_TO)								AS CLEAN_TO
		, rev.PAYMENT_INTERVAL
		, rev.RECURRING_AMOUNT
		, IFF(rev.PAYMENT_INTERVAL = 1, rev.RECURRING_AMOUNT, 0)								AS MRR
		, IFF(rev.PAYMENT_INTERVAL = 3, rev.RECURRING_AMOUNT, 0)								AS QRR
		, IFF(rev.PAYMENT_INTERVAL = 6, rev.RECURRING_AMOUNT, 0)								AS BRR
		, IFF(rev.PAYMENT_INTERVAL = 12, rev.RECURRING_AMOUNT, 0)								AS ARR
		, (QRR/ 3) + MRR																		AS MRR_Q
		, (BRR/ 6) + MRR_Q																		AS MRR_B
		, (ARR/12) + MRR_B																		AS MRR_A
	FROM {{ ref('core_fact_revenue_invoiced') }} rev
	WHERE rev.PAYMENT_INTERVAL <> 0
		AND rev.RECURRING_AMOUNT > 0

)
-- NOTE: Island gap method for the MRR
, invoice_island AS (
SELECT *
	, SUM (CASE WHEN PREVIOUS_MRR_A_ = MRR_A_ THEN 0 ELSE 1 END) OVER (PARTITION BY COMPANY_SK ORDER BY RN_) AS ISLAND_ID
	FROM (
	SELECT *
			, LAG(MRR_A_ ) OVER (PARTITION BY COMPANY_SK ORDER BY CLEAN_FROM, CLEAN_TO)		AS PREVIOUS_MRR_A_
			, ROW_NUMBER () OVER (PARTITION BY COMPANY_SK ORDER BY CLEAN_FROM, CLEAN_TO) 	AS RN_
	FROM (
	SELECT COMPANY_SK
		, CLEAN_FROM
		, CLEAN_TO
		, sum(MRR      ) AS MRR_
		, sum(MRR_Q    ) AS MRR_Q_
		, sum(MRR_B    ) AS MRR_B_
		, sum(MRR_A    ) AS MRR_A_
	FROM invoices
	GROUP BY COMPANY_SK
		, CLEAN_FROM
		, CLEAN_TO
	) A
	) b

)


SELECT
	COMPANY_SK
	, MIN(CLEAN_FROM) 	AS VALID_FROM
	, MAX(CLEAN_TO)		AS VALID_TO
	, MRR_				AS MRR_MONTH
	, MRR_Q_			AS MRR_QUARTER
	, MRR_B_			AS MRR_BIANNUAL
	, MRR_A_ 			AS MRR_ANNUAL
	, MRR_Q_ * 3		AS TOTAL_QUARTER_RECURRING_REVENUE
	, MRR_B_ * 6		AS TOTAL_BIANNUAL_RECURRING_REVENUE
	, MRR_A_ * 12		AS TOTAL_ANNUAL_RECURRING_REVENUE
FROM invoice_island inv
GROUP BY
	COMPANY_SK
	, MRR_
	, MRR_Q_
	, MRR_B_
	, MRR_A_
	, ISLAND_ID