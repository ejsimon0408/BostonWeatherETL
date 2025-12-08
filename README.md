# BostonWeatherETL
Boston Weather ETL Pipeline

This is a cloud-based ETL pipeline that processes historical and real-time weather data for Boston, MA. The pipeline provides actionable insights through automated data processing and an interactive Streamlit dashboard.

The pipeline is production-ready, handles 100MB+ of historical weather data, and demonstrates enterprise-grade data engineering practices including orchestration, cloud storage, and automated monitoring.

Architecture
                     ┌─────────────────────────────┐
                     │         Data Sources        │
                     │                             │
                     │  NOAA Boston Station        │
                     │   (Historical Data)         │
                     │  Open-Meteo API             │
                     │   (Real-time Data)          │
                     └─────────────┬───────────────┘
                                   │
                                   ▼
                     ┌─────────────────────────────┐
                     │        ETL Pipeline         │
                     │                             │
                     │  • Extract                  │
                     │  • Transform                │
                     │  • Load                     │
                     └─────────────┬───────────────┘
                                   │
                                   ▼
                     ┌─────────────────────────────┐
                     │          AWS S3             │
                     │     (Processed Data)        │
                     └─────────────┬───────────────┘
                                   │
                                   ▼
                     ┌─────────────────────────────┐
                     │     Streamlit Dashboard     │
                     │                             │
                     │  • Current Weather Display  │
                     │  • Historical Data Explorer │
                     │  • Temperature Trends       │
                     │  • Anomaly Distribution     │
                     │  • Precipitation Analysis   │
                     │                             │
                     │ Live URL:                   │
                     │ https://bostonweatheretl-   │
                     │ fvgk36nhmvp8uy4hgnxyng.    │
                     │ streamlit.app/              │
                     └─────────────────────────────┘

Data Source and Processing
Data Sources

Historical Weather Data:
Multi-year weather records from NOAA Boston Station, stored in Parquet format on AWS S3.
Includes:

Temperature (TMAX, TMIN)

Precipitation (PRCP)
Source: NOAA National Centers for Environmental Information

Real-time Weather Data:
Current weather from Open-Meteo API.
Includes:

Live temperature readings

Wind speed measurements

Timezone-aware timestamps

ETL Process

The ETL pipeline performs the following steps:

Extraction: Pulls historical data from S3 and real-time data from Open-Meteo API.

Transformation:

Normalizes data across legacy and modern schemas

Converts temperature units (Fahrenheit → Celsius)

Flags temperature anomalies based on historical averages (±3°C threshold)

Pivots data into wide format for efficient analysis

Loading: Saves the processed dataset back to AWS S3 for downstream consumption.

Data Quality Measures

Automatic handling of missing values

Type validation and coercion

Date parsing with error handling

Duplicate detection and removal

Prerequisites

Before running the pipeline or dashboard, ensure the following software is installed:

Python 3.8+

pip (Python package installer)

AWS Account with S3 access

Prefect Cloud account

Git

Installation
1. Clone the Repository
git clone https://github.com/ejsimon0408/boston-weather-etl.git
cd boston-weather-etl

2. Create Virtual Environment

For macOS/Linux:

python -m venv .venv
source .venv/bin/activate


For Windows:

python -m venv .venv
.venv\Scripts\activate

3. Install Dependencies
pip install -r requirements.txt

4. Configure AWS Credentials
aws configure
# Enter your AWS Access Key ID
# Enter your AWS Secret Access Key
# Enter your default region (e.g., us-east-1)

5. Prefect Setup
prefect cloud login
prefect work-pool create "cloud pool" --type process
python BostonWeatherFlow.py

Running the ETL Pipeline
Manual Execution
python BostonWeatherFlow.py

Scheduled Execution

Once deployed, the pipeline runs automatically every 24 hours via Prefect Cloud.

Monitoring

Pipeline status and logs are viewable in Prefect Cloud.

Accessing the Dashboard

View the live published dashboard here: Boston Weather ETL Dashboard

Dashboard Features

Current Weather Display: Latest temperature with anomaly flag

Historical Data Explorer: Year and month filtering

Temperature Trends: Interactive line charts for TMAX/TMIN

Anomaly Distribution: Bar charts showing temperature flags

Precipitation Analysis: Daily precipitation visualization

Project Structure
boston-weather-etl/
├── BostonWeatherFlow.py      # Main ETL pipeline
├── StreamlitDashboard.py     # Interactive dashboard
├── requirements.txt          # Dependencies
├── README.md                 # Project documentation
├── data/                     # Local data directory
└── docs/                     # Documentation and presentations

Configuration
Environment Variables
AWS_ACCESS_KEY_ID=your_access_key
AWS_SECRET_ACCESS_KEY=your-secret_key
AWS_DEFAULT_REGION=us-east-1

S3_BUCKET=weather-etl-emily-2025
S3_INPUT_PREFIX=processed/raw/
S3_OUTPUT_KEY=combined/boston_weather_combined.csv

Pipeline Parameters

Modify BostonWeatherFlow.py as needed:

boston_weather_pipeline(
    s3_bucket="your-bucket-name",
    s3_parquet_prefix="your/prefix/",
    s3_output_bucket="your-output-bucket",
    s3_output_key="path/to/output.csv"
)

Testing
Manual Testing
python BostonWeatherFlow.py
aws s3 ls s3://weather-etl-emily-2025/combined/

Data Validation

Row count validation

Column presence verification

Date range consistency

Temperature range sanity checks

Future Enhancements

Add weather forecasting capabilities

Implement data versioning with DVC

Email notifications for severe weather anomalies

Expand to multiple cities

Machine learning for predictive analytics

Unit and integration tests

Docker containerization

CI/CD pipeline

Acknowledgments

NOAA Boston Station for historical weather data

Open-Meteo API for real-time data

Prefect for workflow orchestration

DACSS 690A instructors and TAs

Author

Emily Simon
DACSS 690A - Fall 2025
GitHub: ejsimon0408

License

This project is created for educational purposes as part of DACSS 690A coursework.
