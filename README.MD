# Market Intelligence Dashboard

An end-to-end data engineering project that collects historical stock market data from Yahoo Finance, transforms it using Python and pandas, stores it in PostgreSQL, and visualizes market trends through an interactive Streamlit dashboard.

The project includes a Dockerized ETL pipeline, a hosted PostgreSQL database, automated testing with GitHub Actions, and scheduled ETL execution.

**Live Dashboard:** [Market Intelligence Dashboard](https://marketintelligenceboard.streamlit.app/)
**Repository:** [Market_Intelligence_Board](https://github.com/gurveerbining-1/Market_Intelligence_Board)

## Project Overview

### 1. Data Extraction and Transformation

The ETL pipeline retrieves historical market data for selected stock tickers using Yahoo Finance and processes it with Python and pandas.

- Extract historical price and volume data for selected companies.
- Reshape market data into a structured format for database storage.
- Calculate daily returns, 7-day moving averages, rolling volatility, price ranges, and normalized prices.
- Load processed data into PostgreSQL using `psycopg2`.
- Use database upserts to update existing records and avoid duplicate rows for the same date and ticker.

### 2. Database Storage

PostgreSQL stores the processed market data in a `market_data` table, with a composite primary key on date and ticker.

The database is hosted on **Neon**, allowing the ETL pipeline and dashboard to access the same database without requiring a locally running PostgreSQL server.

### 3. Automated Testing and CI

The project uses pytest to test the ETL pipeline and validate expected behavior.

- Unit tests for data transformation and processing logic.
- Tests for database-loading behavior, including error handling and rollback scenarios.
- GitHub Actions automatically runs the test suite when changes are pushed to the configured branch or pull requests are opened against it.

### 4. Containerization

Docker packages the ETL script and its Python dependencies into a reproducible environment.

The same container can be run locally or through GitHub Actions, reducing differences between development and scheduled execution environments.

### 5. Scheduled ETL Orchestration

GitHub Actions handles scheduled execution of the ETL pipeline.

- Builds the Docker image and runs the ETL container.
- Retrieves database credentials from GitHub repository secrets.
- Supports manual execution for testing and troubleshooting.
- Runs on a configured schedule without requiring a continuously running local machine or self-hosted server.

### 6. Interactive Dashboard

The Streamlit dashboard is deployed on Streamlit Community Cloud and connects to the hosted PostgreSQL database.

- Filter market data by ticker and date range.
- Explore historical closing prices and market trends.
- Analyze daily return distributions.
- Compare trading volume across selected companies.

Plotly provides interactive charts for exploring the data.

## Architecture

```text
Yahoo Finance
     |
     v
Python ETL Pipeline
(Pandas transformations)
     |
     v
Docker Container
     |
     v
Neon PostgreSQL <------- GitHub Actions
     |                   (scheduled ETL runs)
     v
Streamlit Dashboard
(Community Cloud)

GitHub Actions
     |
     v
pytest test suite
(CI on code changes)
```

## Technology Stack

| Category              | Technologies                                             |
| --------------------- | -------------------------------------------------------- |
| Language              | Python                                                   |
| Data source           | Yahoo Finance, yfinance                                  |
| Data processing       | pandas                                                   |
| Database              | PostgreSQL, Neon                                         |
| Database connectivity | psycopg2                                                 |
| Dashboard             | Streamlit, Plotly                                        |
| Testing               | pytest                                                   |
| Containerization      | Docker                                                   |
| CI and orchestration  | GitHub Actions                                           |
| Configuration         | Environment variables, Streamlit Secrets, GitHub Secrets |

## Getting Started

### Prerequisites

- Python 3.12 or a compatible Python version
- Git
- Docker, if running the ETL pipeline in a container
- A PostgreSQL database, such as a Neon database, for loading and viewing data

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

Install the dependencies:

```bash
python -m pip install -r requirements.txt
```

For development and testing, install the development dependencies:

```bash
python -m pip install -r requirements-dev.txt
```

### 3. Configure the Database Connection

Create a `.env` file in the project root and provide your PostgreSQL connection string:

```dotenv
DB_URL=postgresql://USERNAME:PASSWORD@HOST/DATABASE?sslmode=require
```

Replace the placeholders with your own database credentials and connection details. For Neon, use the connection string provided by your Neon project.

The ETL script and dashboard use `DB_URL` to connect to the database. Ensure the database is accessible and the required permissions are configured.

**Security:** Never commit `.env` files, database passwords, or production connection strings to version control.

### 4. Run the ETL Pipeline Locally

With your virtual environment activated and `.env` configured, run:

```bash
python ETL_Yahoo_Finance.py
```

The script retrieves market data, calculates the derived metrics, and inserts or updates records in PostgreSQL.

### 5. Run the ETL Pipeline with Docker

Build the Docker image:

```bash
docker build -t market-etl .
```

Run the container using your local `.env` file:

```bash
docker run --rm --env-file .env market-etl
```

The container runs the ETL script and exits when processing finishes.

### 6. Launch the Dashboard Locally

Ensure the database connection is configured, then start Streamlit:

```bash
streamlit run dashboard.py
```

Open the local URL printed in the terminal to interact with the dashboard.

### 7. Run the Tests

Execute the test suite:

```bash
python -m pytest
```

GitHub Actions also runs the tests automatically according to the configured CI workflow.

## Deployment and Automation

### Streamlit Community Cloud

The dashboard is deployed to Streamlit Community Cloud. Configure `DB_URL` in the application's Secrets settings so the hosted dashboard can connect to Neon.

### GitHub Actions

Two workflows handle testing and scheduled execution:

- **CI workflow:** Runs the pytest suite on configured code changes and pull requests.
- **Scheduled ETL workflow:** Builds and runs the Dockerized ETL pipeline using the `DB_URL` repository secret. The workflow can also be triggered manually through the GitHub Actions interface.

The schedule is configured in `.github/workflows/scheduled-etl.yml`. Check that file for the current execution time and days.

## Security Considerations

- Keep `.env` files out of version control.
- Store deployment credentials in Streamlit Secrets and GitHub Actions secrets.
- Use encrypted database connections where supported.
- Apply appropriate database permissions and avoid exposing unnecessary services publicly.
- Never print database credentials or connection strings in workflow logs.

## Future Improvements

- Add explicit data-quality validation and freshness checks.
- Improve ETL observability with structured logging and notifications on failure.
- Add dashboard metrics for data freshness and pipeline execution status.
- Expand the pipeline with additional data sources or analytical features.
- Add more robust integration tests and database schema management.

## Author

**Gurveer Bining**

[GitHub Profile](https://github.com/gurveerbining-1)

