Market Intelligence Dashboard

**Install PostgreSQL** (if not pre-installed) and set up Apache Airflow. For Airflow, install it via `pip`, then configure systemd services for the scheduler and webserver so that they startup automaticall. Access the webserver using an SSH tunnel.

1. Create a **Python script** to:

- Extract data from Yahoo Finance.

- Perform transformations (e.g., group data by week and calculate averages/medians).

- Load the transformed data into a PostgreSQL table using `psycopg2`-bin or sql alchemy.

2. Write **tests** for each stage (Extract, Transform, Load) using `pytest`. In one of the tests query the table and make sure that all the columns exist, check the types, make sure that the rows for the most recent date are there, etc...

Note: Testing is a huge part, and is often overlooked by beginners.

3. **Dockerize** the Python script(s).

4. Create an **Airflow DAG** (Python file) to schedule the Docker container daily using either the BashOperator or DockerOperator. (both operators work fine)

5. Build a **Streamlit dashboard** that:

- Loads data from PostgreSQL.

- Has filters to filter the data (date range filter, text input filter for the tickers, etc...)

- Displays visualizations (can use plotly express.)

Ensure the correct VM ports are open for database access but be mindful of security, don't expose ip addresses, ports, etc... Lookup best safety practices for working with cloud vms. If sharing the project, consider exposing data through an API or using other safer methods.

**In summary**:

- Create a Python script for ETL (Yahoo Finance → pandas → PostgreSQL).

- Dockerize it.

- Schedule it with Airflow.

- Build a Streamlit dashboard for visualization.

Take all necessary precautions to secure the database and application when hosting online.

