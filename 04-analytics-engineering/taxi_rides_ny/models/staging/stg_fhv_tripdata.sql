{{
    config(
        materialized='view'
    )
}}

-- can't add a tripid because dispatching_base_num + pickup location id is not unique.
select

    dispatching_base_num,
    {{ dbt.safe_cast("pulocationid", api.Column.translate_type("integer")) }} as pickup_locationid,
    {{ dbt.safe_cast("dolocationid", api.Column.translate_type("integer")) }} as dropoff_locationid,
    cast(pickup_datetime as timestamp) as pickup_datetime,
    cast(dropoff_datetime as timestamp) as dropoff_datetime,

    sr_flag

from {{ source('staging', 'fhv_tripdata') }}
where pickup_datetime >= '2019-01-01'
and pickup_datetime < '2020-01-01'

-- dbt build --select <model_name> --vars '{'is_test_run': 'false'}'
{% if var('is_test_run', default=true) %}

  limit 100

{% endif %}