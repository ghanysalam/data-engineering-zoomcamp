with green_data as (
    select * from {{ ref('stg_green_tripdata') }}
), 

yellow_data as (
    select * from {{ ref('stg_yellow_tripdata') }}
), 

trips_unioned as (
    select * from green_data
    union all
    select * from yellow_data
)

select distinct pickup_location_id from trips_unioned