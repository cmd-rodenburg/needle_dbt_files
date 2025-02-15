SELECT DATE 							AS DATE_KEY
	, {{ dbt_utils.star(from = ref('gemma_dates'), except=['DATE']) }}
	, CASE -- fixed holidays Berlin
		WHEN RIGHT(DATE_ID,4) IN ('0101'
			,	'0308'
			,	'0501'
			,	'1003'
			,	'1225'
			,	'1226') THEN TRUE
		-- EASTER 2021/22/23
		WHEN DATE_ID IN ('20210405'
			, '20210404'
			, '20210402'
			, '20220418'
			, '20220417'
			, '20220415'
			, '20230410'
			, '20230409'
			, '20230407') THEN TRUE
		-- ASCENSION DAY AND WHIT MONDAY 2021/22/23
		WHEN DATE_ID IN ('20210513'
		, '20220526'
		, '20230518'
		, '20210524'
		, '20220606'
		, '20230529') THEN TRUE
		ELSE FALSE
	END									AS IS_HOLIDAY
FROM {{ ref('gemma_dates') }}

