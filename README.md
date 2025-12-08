# Boston Weather ETL Pipeline

A production-ready, cloud-based ETL pipeline that processes historical and real-time weather data for Boston, providing actionable insights through automated data processing and an interactive Streamlit dashboard.

---

## Project Overview

This end-to-end data engineering solution integrates multiple data sources, processes over 100MB of historical weather data, and delivers insights through an interactive Streamlit dashboard. The pipeline demonstrates enterprise-grade data engineering practices including orchestration, cloud storage, and automated monitoring.

### Business Value

- **Real-time Weather Insights**: Automated processing of current weather conditions  
- **Historical Trend Analysis**: Comparative analysis against multi-year historical baselines  
- **Anomaly Detection**: Automated flagging of temperature anomalies for decision support  
- **Scalable Architecture**: Cloud-native design supporting growth and additional data sources  

---

## Architecture

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
                     │         AWS S3              │
                     │    (Raw Data Storage)       │
                     │ NOAA data placed here       │
                     └─────────────┬───────────────┘
                                   │
                                   ▼
                     ┌─────────────────────────────┐
                     │       AWS Glue Job          │
                     │  Parses raw NOAA data into  │
                     │  structured buckets         │
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
                     │ https://bostonweatheretl-fvgk36nhmvp8uy4hgnxyng.streamlit.app/ │
                     └─────────────────────────────┘

---

## Technologies Used

### Core Technologies

1. **Prefect** - Workflow orchestration and monitoring  
2. **AWS S3** - Cloud data storage and retrieval  
3. **AWS Glue** - Data parsing and transformation  
4. **Pandas** - Data transformation and processing  
5. **Streamlit** - Interactive data visualization  
6. **Boto3** - AWS service integration  

### Additional Tools

- **Altair** - Advanced data visualization  
- **NumPy** - Numerical computations  
- **Requests** - API integration  

---

## Data Processing

### Data Sources

- **Historical Data**: Multi-year weather records from **NOAA Boston Station**, initially pulled into **S3 raw buckets**, then parsed and structured via an **AWS Glue job**.  
  - Temperature (TMAX, TMIN)  
  - Precipitation (PRCP)  
  *Source: [NOAA National Centers for Environmental Information](https://www.ncei.noaa.gov/)*  

- **Real-time Data**: Current weather from Open-Meteo API  
  - Live temperature readings  
  - Wind speed measurements  
  - Timezone-aware timestamps  

### Transformations

- **Data Normalization**: Unified format across legacy and modern data schemas  
- **Temperature Conversion**: Fahrenheit to Celsius standardization  
- **Anomaly Detection**: Statistical flagging based on historical averages  
  - Daily comparison (same calendar day across all years)  
  - Monthly comparison (same month across all years)  
  - ±3°C threshold for "Above/Below Average" classification  
- **Data Pivoting**: Wide-format conversion for efficient analysis  

### Data Quality

- Automatic handling of missing values  
- Type validation and coercion  
- Date parsing with error handling  
- Duplicate detection and removal  

---

## Prerequisites

- Python 3.8+  
- AWS Account with S3 and Glue access  
- Prefect Cloud account  
- Git  

---

## Installation & Setup

### 1. Clone the Repository

git clone https://github.com/ejsimon0408/boston-weather-etl.git  
cd boston-weather-etl  

### 2. Create a Virtual Environment

For macOS/Linux:  
python -m venv .venv  
source .venv/bin/activate  

For Windows:  
python -m venv .venv  
.venv\Scripts\activate  

### 3. Install Dependencies

pip install -r requirements.txt  

### 4. Configure AWS Credentials

aws configure  
# Enter your AWS Access Key ID  
# Enter your AWS Secret Access Key  
# Enter your default region (e.g., us-east-1)  

### 5. Prefect Setup

prefect cloud login  
prefect work-pool create "cloud pool" --type process  
python BostonWeatherFlow.py  

---

## Running the ETL Pipeline

### Manual Execution

python BostonWeatherFlow.py  

### Scheduled Execution

Once deployed, the pipeline runs automatically every 24 hours via Prefect Cloud.  

### Monitoring

Pipeline status and logs are viewable in Prefect Cloud.  

---

## Accessing the Dashboard

View the **live published dashboard** here: [Boston Weather ETL Dashboard](https://bostonweatheretl-fvgk36nhmvp8uy4hgnxyng.streamlit.app/)

### Dashboard Features

- Current Weather Display: Latest temperature with anomaly flag  
- Historical Data Explorer: Year and month filtering  
- Temperature Trends: Interactive line charts for TMAX/TMIN  
- Anomaly Distribution: Bar charts showing temperature flags  
- Precipitation Analysis: Daily precipitation visualization  

---

## Project Structure

boston-weather-etl/  
├── BostonWeatherFlow.py      # Main ETL pipeline  
├── Dashboard.py              # Interactive dashboard  
├── requirements.txt          # Python dependencies  
├── README.md                 # Project documentation   
└── docs/                     # Documentation and presentations  

---

## Configuration

### Environment Variables

AWS_ACCESS_KEY_ID=your_access_key  
AWS_SECRET_ACCESS_KEY=your_secret_key  
AWS_DEFAULT_REGION=us-east-1  

S3_BUCKET=weather-etl-emily-2025  
S3_INPUT_PREFIX=processed/raw/  
S3_OUTPUT_KEY=combined/boston_weather_combined.csv  

### Pipeline Parameters

Modify in BostonWeatherFlow.py:  

boston_weather_pipeline(  
    s3_bucket="your-bucket-name",  
    s3_parquet_prefix="your/prefix/",  
    s3_output_bucket="your-output-bucket",  
    s3_output_key="path/to/output.csv"  
)  

---

## Testing

### Manual Testing

python BostonWeatherFlow.py  
aws s3 ls s3://weather-etl-emily-2025/combined/  

### Data Validation Checks

- Row count validation  
- Column presence verification  
- Date range consistency  
- Temperature range sanity checks  

---

## Future Enhancements

- Add weather forecasting capabilities  
- Implement data versioning with DVC  
- Email notifications for severe weather anomalies  
- Expand to multiple cities  
- Machine learning for predictive analytics  
- Unit and integration tests  
- Docker containerization  
- CI/CD pipeline  

---

## Acknowledgments

- NOAA Boston Station for historical weather data  
- Open-Meteo API for real-time data  
- AWS Glue for parsing raw NOAA data  
- Prefect for workflow orchestration  
- DACSS 690A instructors and TAs  

---

## Author

**Emily Simon**  
DACSS 690A - Fall 2025  
GitHub: [ejsimon0408](https://github.com/ejsimon0408)  

---

## License

This project is created for educational purposes as part of DACSS 690A coursework.
