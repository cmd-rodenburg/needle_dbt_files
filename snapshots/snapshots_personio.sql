
{% snapshot snap_personio_employees %}

  {{ config(
    target_schema=generate_schema_name('snapshots'),
    strategy='check',
    unique_key='"id"',
    check_cols= ['"id"','"first_name"','"last_name"','"gender"','"status"','"position"','"employment_type"','"weekly_working_hours"','"hire_date"','"termination_date"','"termination_type"','"probation_period_end"','"department"','"team"','"dynamic_181223"','"contract_end_date"']
    )
  }}
  SELECT * FROM {{ source('personio', 'employees') }}
{% endsnapshot %}
