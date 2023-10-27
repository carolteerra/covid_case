--What is the country with the highest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?

SELECT
    country,
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000",
    year_week
FROM
    covid_data.country_covid_cases_view cccv  
WHERE
    year_week = '2020-31'
ORDER BY
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" DESC
LIMIT 1;

--What is the top 10 countries with the lowest number of Covid-19 cases per 100 000 Habitants at 31/07/2020?
SELECT
    country,
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000",
    year_week
FROM
    covid_data.country_covid_cases_view cccv  
WHERE
    year_week = '2020-31'
    and "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" is not null
ORDER BY
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" ASC
LIMIT 10;

--What is the top 10 countries with the highest number of cases among the top 20 richest countries (by GDP per capita)? (the question does not especify a point in time, so I did the query regarding the most up to date information)
with top_20 as (SELECT
    country,
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000",
    gdp__per_capita
FROM
    covid_data.country_covid_cases_view cccv  
WHERE 
	"Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" is not null
	AND year_week  = (
        SELECT MAX(year_week)
        FROM covid_data.country_covid_cases_view
        WHERE "indicator" = 'cases'
    )
ORDER by
	gdp__per_capita desc LIMIT 20)	
select * from top_20
order by "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" DESC
limit 10

--List all the regions with the number of cases per million of inhabitants and display information on population density, for 31/07/2020
SELECT
    region,
    SUM(pop_density_per_sq_mi) AS total_pop_density,
    SUM("Cumulative_number_for_14_days_of_COVID_19_cases_per_100000") * 10 AS total_cumulative_cases_per_1000000
FROM
    covid_data.country_covid_cases_view cccv
WHERE
    year_week = '2020-31'
GROUP BY
    region;

--Query the data to find duplicated records
SELECT *, count(*)
FROM covid_data.country_covid_cases_view cccv
group by cccv.country , cccv.region , cccv.population , cccv.area_sq_mi , cccv.pop_density_per_sq_mi ,cccv.coastline_coastarea_ratio ,cccv.net_migration ,cccv.infant_mortality_per_1000_births, cccv.gdp__per_capita ,cccv.literacy ,cccv.phones_per_1000 ,cccv.arable ,cccv.crops ,cccv.other ,cccv.climate ,cccv.birthrate, cccv.deathrate ,cccv.agriculture ,cccv.industry ,cccv.service ,cccv.updated_at ,cccv.cumulative_count ,cccv.year_week ,cccv."indicator" ,cccv."Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" 
HAVING COUNT(*) > 1

--explain the performance of all the queries and describes what you see. Get improvements suggestions.
explain SELECT
    country,
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000",
    year_week
FROM
    covid_data.country_covid_cases_view cccv  
WHERE
    year_week = '2020-31'
ORDER BY
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" DESC
LIMIT 1;


explain SELECT
    country,
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000",
    year_week
FROM
    covid_data.country_covid_cases_view cccv  
WHERE
    year_week = '2020-31'
    and "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" is not null
ORDER BY
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" ASC
LIMIT 10;

explain SELECT
    country,
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000",
    gdp__per_capita
FROM
    covid_data.country_covid_cases_view cccv  
WHERE 
	"Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" is not null
	AND year_week  = (
        SELECT MAX(year_week)
        FROM covid_data.country_covid_cases_view
        WHERE "indicator" = 'cases'
    )
ORDER by
	gdp__per_capita desc, 
    "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" DESC
LIMIT 20;

explain SELECT
    region,
    SUM(pop_density_per_sq_mi) AS total_pop_density,
    SUM("Cumulative_number_for_14_days_of_COVID_19_cases_per_100000") * 10 AS total_cumulative_cases_per_1000000
FROM
    covid_data.country_covid_cases_view cccv
WHERE
    year_week = '2020-31'
GROUP BY
    region;
   
explain SELECT *, count(*)
FROM covid_data.country_covid_cases_view cccv
group by cccv.country , cccv.region , cccv.population , cccv.area_sq_mi , cccv.pop_density_per_sq_mi ,cccv.coastline_coastarea_ratio ,cccv.net_migration ,cccv.infant_mortality_per_1000_births, cccv.gdp__per_capita ,cccv.literacy ,cccv.phones_per_1000 ,cccv.arable ,cccv.crops ,cccv.other ,cccv.climate ,cccv.birthrate, cccv.deathrate ,cccv.agriculture ,cccv.industry ,cccv.service ,cccv.updated_at ,cccv.cumulative_count ,cccv.year_week ,cccv."indicator" ,cccv."Cumulative_number_for_14_days_of_COVID_19_cases_per_100000" 
HAVING COUNT(*) > 1;
