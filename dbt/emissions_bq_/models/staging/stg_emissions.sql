{{config(materialized='table')}}

with country_codes as(
                    select * 
                    from {{source('raw_to_stg','codes')}}
                    ),

    emissions_data as(
                    select * 
                    from {{source('raw_to_stg','emissions')}}
                    )


select 	e.ID,
        Country_name,
        Vehicle_Family_ID,	
        Manufacturer_OEM,
        Make,
        Commercial_Name,
        Mass_RO,
        Emissions_WLTP,
        Fuel_type,
        Fuel_mode,
        Engine_capacity,
        Engine_power,
        Electricity_consumption,
        Registration_date,
        Fuel_consumption,
        Electric_range


FROM emissions_data e

join country_codes c  on e.Country = c.Country_code
order by e.ID 
