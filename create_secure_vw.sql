use warehouse <YOUR_WH>;
use schema citibike.public;
--lets take a quick look at the trips and weather tables
--the trips table contains trip info from the Citibike NYC bike sharing program
select * from trips limit 100;
--the weather table contains JSON weather data
select * from weather limit 100;

----------------------------------------------------------------------------------
-- Create a secure view with trip (structured) and weather (semistructured) data
----------------------------------------------------------------------------------
-- **Make sure to give your view a unique name**
create or replace secure view <unique_view_name> as
select *
from trips
  left outer join
  (select t as observation_time
    ,v:city.id::int as city_id
    ,v:city.name::string as city_name
    ,v:city.country::string as country
    ,v:city.coord.lat::float as city_lat
    ,v:city.coord.lon::float as city_lon
    ,v:clouds.all::int as clouds
    ,(v:main.temp::float)-273.15 as temp_avg_c
    ,(v:main.temp_min::float)-273.15 as temp_min_c
    ,(v:main.temp_max::float)-273.15 as temp_max_c
    ,(v:main.temp::float)*9/5-459.67 as temp_avg_f
    ,(v:main.temp_min::float)*9/5-459.67 as temp_min_f
    ,(v:main.temp_max::float)*9/5-459.67 as temp_max_f
    ,v:weather[0].id::int as weather_id
    ,v:weather[0].main::string as weather
    ,v:weather[0].description::string as weather_desc
    ,v:weather[0].icon::string as weather_icon
    ,v:wind.deg::float as wind_dir
    ,v:wind.speed::float as wind_speed
    from weather
    where city_id = 5128638)
  on date_trunc(HOUR, starttime) = date_trunc(HOUR, observation_time);

--test the secure view
select * from <unique_view_name> where date_part('year', observation_time)=2018 limit 20;

-- our API is configured to use the user 'snowflake_api' whose default role is also named 'snowflake-api'
-- now lets make sure that the 'snowflake-api' role has access to this newly created secure view
use role snowflake_api;
select * from <unique_view_name> where date_part('year', observation_time)=2018 limit 20;
