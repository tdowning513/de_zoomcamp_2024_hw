{{
    config(
        materialized='table'
    )
}}

select
    fhvtrip.dispatching_base_num,
    fhvtrip.pickup_locationid,
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    fhvtrip.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    fhvtrip.pickup_datetime, 
    fhvtrip.dropoff_datetime, 
    fhvtrip.sr_flag
from {{ ref('stg_fhv_tripdata') }} fhvtrip
join {{ ref('dim_zones') }} pickup_zone on fhvtrip.pickup_locationid = pickup_zone.locationid and pickup_zone.borough != 'Unknown'
join {{ ref('dim_zones') }} dropoff_zone on fhvtrip.dropoff_locationid = dropoff_zone.locationid and dropoff_zone.borough != 'Unknown'

