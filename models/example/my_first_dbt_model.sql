
/*
    Welcome to your first dbt model!
    Did you know that you can also configure models directly within SQL files?
    This will override configurations stated in dbt_project.yml

    Try changing "table" to "view" below
*/

{{ config(materialized='table') }}
WITH parent_query AS (
select f.amount,d.country from workspace.gold.factbookings f
left join workspace.gold.dimairports d on f.DimAirportsKey = d.DimAirportsKey )
select country,sum(amount) as total_amount
from parent_query
group by country 

/*
    Uncomment the line below to remove records with null `id` values
*/

-- where id is not null
