{% set records = [
       ['2021-12-13',   1,  true,  35,  'new']
     , ['2021-12-14',   1,  true,	20,  'downgrade']
     , ['2021-12-15',   1,  false,	0,  'churn']
     , ['2021-12-16',   1,  false,	0,  'none']
     , ['2021-12-17',   1,  false,	0,  'none']
     , ['2021-12-18',   1,  false,	0,  'none']
     , ['2021-12-19',   1,  false,	0,  'none']
     , ['2021-12-20',   1,  false,	0,  'none']
     , ['2021-12-21',   1,  true,	39,  'reactivation']
     , ['2021-12-22',   1,  false,	0,  'churn']

     , ['2021-12-16',	 2,  true,	15,	'new']
     , ['2021-12-17',   2,  true,	15,	'none']
     , ['2021-12-18',   2,  true,	20,	'upgrade']
     , ['2021-12-19',   2,  true,	20,	'none']
     , ['2021-12-20',   2,  true,	15,	'downgrade']
     , ['2021-12-21',   2,  true,	15,	'none']
     , ['2021-12-22',   2,  true,	15,	'none']
     , ['2021-12-23',   2,  true,	15,	'none']

 ] %}

 {% for record in records %}
     SELECT
     CAST('{{ record[0] }}' AS DATE) AS date
     , {{ record[1] }} AS hubspot_company_id
     , {{ record[2] }} AS is_active
     , {{ record[3] }} AS monthly_revenue
     , '{{ record[4] }}' AS change_category
     {% if not loop.last %}
         UNION ALL
     {% endif %}
 {% endfor %}
