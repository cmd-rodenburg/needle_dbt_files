
{% snapshot snap_hubspot_companies %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'companies') }}
{% endsnapshot %}

{% snapshot snap_hubspot_contacts %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot_engagements', 'contacts') }}
{% endsnapshot %}

{% snapshot snap_hubspot_deals %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'deals') }}
{% endsnapshot %}

{% snapshot snap_hubspot_feedback_submissions %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'feedback_submissions') }}
{% endsnapshot %}

{% snapshot snap_hubspot_line_items %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'line_items') }}
{% endsnapshot %}

{% snapshot snap_hubspot_owners %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'owners') }}
{% endsnapshot %}

{% snapshot snap_hubspot_pipelines %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'pipelines') }}
{% endsnapshot %}

{% snapshot snap_hubspot_products %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'products') }}
{% endsnapshot %}

{% snapshot snap_hubspot_properties %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT
      {{ dbt_utils.surrogate_key(['"object_type"', '"name"']) }} AS "id"
    , *
   FROM {{ source('hubspot', 'properties') }}
{% endsnapshot %}

{% snapshot snap_hubspot_quotes %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'quotes') }}
{% endsnapshot %}

{% snapshot snap_hubspot_tickets %}
  {{ config(
    target_schema=generate_schema_name('snapshots'),
    unique_key='"id"',
    strategy="timestamp",
    updated_at='"updatedAt"',
  ) }}
  SELECT * FROM {{ source('hubspot', 'tickets') }}
{% endsnapshot %}
