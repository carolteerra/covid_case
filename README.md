
# Business case for Dell    

This folder answers to the list of exercises proposed by Dell as a business case for a Data Engineer application. 



## Requisites

The requisites for testing the codes in this folder are the following:

1. Installed and configured [PostgreSQL](https://www.postgresql.org/download/);

2. Installed [Python 3](https://www.python.org/downloads/) and the modules [Psycopg2](https://www.psycopg.org/docs/) and [SQLAlchemy](https://docs.sqlalchemy.org/en/20/intro.html#installation).


## Exercise 1

To run script in exercise_1.py simply download this repository and, once inside it, execute the following command:

```python3 exercise_1.py```

Or execute it with the IDE of your choice. Do not forge to replace database credentials for your own for appropriate connection. 


## Exercise 2

:warning: To avoid error during the INSERT command, execute this SQL command using psql CLI or in your preferred database administration tool replacing it with your schema and table names: :warning:

```
ALTER TABLE your_schema_name.your_table_name
ADD CONSTRAINT unique_key UNIQUE (country, year_week, "indicator");
```

The script exercise_2.py can be ran in the command line with 

```python3 exercise_2 ```

Or executed in your preferred IDE. Do not forget to replace the database credentials for appropriate connection.

However, the script is supposed to be scheduled. To do so:

* **In Linux systems**:

 ```crontab -e ```

 and copy the following to your cron file, replacing the path with yours locally:
 ```
 55 23 * * * /path/to/python3 /path/to/your_script.py
 ``` 

 This will ensure execution at 23:55 every day. You can alter this schedule to your preference.

* **In Windows**:

    1. Open the "Task Scheduler":
    - Press `Win + S` to open the Windows search bar.
    - Type "Task Scheduler" and press `Enter`.

    2. In the "Task Scheduler," in the left panel, right-click on "Task Scheduler Library" and choose "Create Folder." Give the folder a name (optional) and click "OK."

    3. Now, inside the folder you just created (or directly in the "Task Scheduler Library"), right-click and select "Create Basic Task..."

    4. Follow the task creation wizard. Give your task a name and description.

    5. Choose the frequency at which you want to run the task (once a day) and click "Next."

    6. Set the start date and time, and specify the repetition frequency for the task. Click "Next."

    7. Select the option "Start a program" and click "Next."

    8. Specify the path to the Python executable and the path to your script. For example:
   - Program/script: `C:\path\to\python.exe`
   - Add arguments (optional): `C:\path\to\your_script.py`

    9. Review the task settings, click "Finish," and the task will be scheduled.

   Make sure you have the necessary permissions to run the program or script you're scheduling. Also, ensure that Python is installed on your system and the path to the Python interpreter is correctly set up.
## Exercise 3

Run the SQL script exercise_3.sql using CLI or your prefered database administration tool.

## Exercise 4

Run the SQL script exercise_4.sql using CLI or your prefered database administration tool.

#### Describing and making suggestion based on the explain performances

The hash join operations represent most of the cost for nearly all queries, except for the last one, that begins with a HashAggregate operation. It groups the results based on a large number of columns, which indicates a complex grouping key. The second factor weighting overall query performances are Aggregate and GroupAggregate operations.

As means to imporve the join operations' performances,  ensuring that proper indexes are created on columns used in filter conditions can bring benefits to the long term usage of queries as the database grows in size. Aggregation issues can also be improved by filters, besides filtering data before aggregation.

Partitioning tables as they grow can also become useful as the amount of data store increases, as this can speed up both aggregation and querying.
## Exercise 5

The data was enriched by the dataset regarding COVID-19 Vaccinations across the globe throughout time provenient from [Our World In Data](https://ourworldindata.org/covid-vaccinations). 

Enriching datasets on COVID-19 cases with comprehensive country-specific socioeconomic, demographic, and vaccination rate data over time is of paramount relevance in understanding the multifaceted impact of the pandemic on global populations. By considering vaccination rates, it becomes possible to assess the efficacy of public health interventions, identify vulnerable populations, and predict future outbreak scenarios. Moreover, by combining these datasets, it enables a holistic examination of how countries with distinct socioeconomic characteristics and vaccination strategies experience and respond to the pandemic differently. This comprehensive insight can facilitate more informed decision-making, targeted resource allocation, and the development of more effective public health measures on a global scale, ultimately aiding in the battle against COVID-19, prevention from further outbreaks and management of possible future worst case scenarios in light of the lessons learned.
## Exercise 6


1. **Countries with higher GDP per capita have higher vaccination coverage (fully vaccinated peole relative to population)**

According to data from Our World in Data, countries with higher GDP per capita have, on average, higher vaccination coverage. For example, on October 25, 2023, the country with the highest GDP per capita, Luxembourg, had a vaccination coverage of 71,45%, while the country with the lowest GDP per capita, Sierra Leone, had a vaccination coverage of 57,5%.

2. **More densily populated countries have more cummulative cases in proportion to population**

Namibia and Mongolia, the two least populated countries have proportionately less cummulativecases in relation to population than the two most populated coutnries, Monaco and Singapore.

3. **Case fatality is lower among countries with higher vaccination coverage than among those with lower vaccination rates**

The countries with most fatal cases, Syria, Sudan and Yemen, all have vaccination rates under 30%. The countries with least fatal cases, Bhutan, Singapore and Iceland all have vaccination rates above 80%.