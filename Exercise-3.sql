CREATE OR REPLACE VIEW covid_data.country_covid_cases_view AS
WITH LatestCases AS (
    SELECT
        ndnrc.country as country,
        ndnrc.cumulative_count,
        ndnrc.year_week,
        ndnrc."indicator"
    FROM covid_db.covid_data.national_14day_notification_rate_covid_19 AS ndnrc
    WHERE ndnrc."indicator" = 'cases'
    AND ndnrc.updated_at = (
        SELECT MAX(updated_at)
        FROM covid_db.covid_data.national_14day_notification_rate_covid_19
        WHERE "indicator" = 'cases'
    )
)
SELECT
    cotw.*,
    lc.cumulative_count,
    lc.year_week,
    lc."indicator",
    (lc.cumulative_count / cotw.population) * 100000 AS "Cumulative_number_for_14_days_of_COVID_19_cases_per_100000"
FROM covid_db.country_data.countries_of_the_world AS cotw
LEFT JOIN LatestCases AS lc ON cotw.country = lc.country;