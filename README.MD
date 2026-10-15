# Market Intelligence Dashboard

An end-to-end data engineering project that collects stock market data from Yahoo Finance, transforms and aggregates it using Python and pandas, stores it in PostgreSQL, and visualizes the results through an interactive Streamlit dashboard.

## Project Overview

The project demonstrates a complete data pipeline, from data extraction and transformation to database storage, automated testing, and interactive visualization.

### 1. Data Extraction and Transformation

The ETL pipeline retrieves historical stock market data from Yahoo Finance and processes it using Python and pandas.

- Extract stock market data for selected tickers.
- Transform and aggregate the data, including weekly averages and medians.
- Load the processed data into PostgreSQL using SQLAlchemy and psycopg2.

### 2. Automated Testing

The project uses pytest to validate the ETL pipeline and help ensure data integrity.

Tests cover the extraction, transformation, and loading stages, including checks on the resulting database data and schema.

### 3. Containerization

Docker is used to package the ETL application and its dependencies into a reproducible runtime environment.

### 4. Workflow Orchestration

Apache Airflow is planned for scheduling and monitoring automated ETL runs. The orchestration setup will be configured as part of the deployment process.

### 5. Interactive Dashboard

The Streamlit dashboard connects to PostgreSQL and provides an interface for exploring the processed market data.

- Filter data by date range and stock ticker.
- Explore historical market trends through interactive visualizations.
- Compare market data across selected companies.

## Technology Stack

- **Language:** Python
- **Data source:** Yahoo Finance
- **Data processing:** pandas
- **Database:** PostgreSQL
- **Database connectivity:** SQLAlchemy, psycopg2
- **Dashboard:** Streamlit, Plotly
- **Testing:** pytest
- **Containerization:** Docker
- **Planned orchestration:** Apache Airflow

## Getting Started

### Prerequisites

- Python and pip
- PostgreSQL
- Git
- Docker, if running the ETL pipeline in a container

### 1. Clone the Repository

```bash
git clone https://github.com/gurveerbining-1/Market_Intelligence_Board.git
cd Market_Intelligence_Board
```

### 2. Set Up the Python Environment

Create and activate a virtual environment.

**Linux / WSL:**

```bash
python3 -m venv .venv
source .venv/bin/activate
```

**Windows PowerShell:**

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
```

Install the project dependencies:

```bash
python -m pip install -r requirements.txt
```

If you are setting up the development environment, install the development dependencies as well:

```bash
python -m pip install -r requirements-dev.txt
```

### 3. Configure PostgreSQL

Create a PostgreSQL database and configure the connection credentials using the environment variables expected by the application.

Example `.env` file:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=your_database
DB_USER=your_username
DB_PASSWORD=your_password
```

Make sure PostgreSQL is running and the database exists before executing the pipeline or launching the dashboard.

**Security:** Keep credentials in your local `.env` file or a secrets manager. Never commit passwords or production connection strings to version control.

### 4. Run the ETL Pipeline

Execute the ETL script to retrieve, transform, and load the market data:

```bash
python ETL_Yahoo_Finance.py
```

Ensure the database connection is configured correctly before running the script.

### 5. Launch the Dashboard

Start the Streamlit application:

```bash
streamlit run dashboard.py
```

Open the local URL printed in the terminal to interact with the dashboard.

### 6. Run the Tests

Execute the automated test suite:

```bash
python -m pytest
```

Run the tests after modifying the ETL pipeline or database-related logic to help catch regressions.

## Security Considerations

- Do not commit `.env` files or credentials.
- Avoid exposing PostgreSQL directly to the public internet.
- Use restricted database permissions.
- Keep production secrets separate from local development settings.

## Future Improvements

- Deploy the Streamlit dashboard and PostgreSQL database to hosted infrastructure.
- Automate test execution with GitHub Actions.
- Configure scheduled ETL execution and monitoring.
- Improve pipeline reliability with additional data-quality checks and failure handling.

## Author

**Gurveer Bining**

[GitHub Profile](https://github.com/gurveerbining-1)
