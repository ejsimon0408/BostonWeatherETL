from prefect import flow, task, get_run_logger
import pandas as pd
import requests
import numpy as np
import boto3
from io import BytesIO

@task(retries=3, retry_delay_seconds=5, log_prints=True)
def extract_from_api(lat: float = 42.3601, lon: float = -71.0589) -> dict:
    logger = get_run_logger()
    logger.info(f"Fetching live weather data for lat={lat}, lon={lon}...")

    url = (
        f"https://api.open-meteo.com/v1/forecast?"
        f"latitude={lat}&longitude={lon}&current_weather=true"
    )

    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        current = response.json().get("current_weather", {})

        if "time" in current:
            dt = pd.to_datetime(current["time"]).tz_localize("UTC").tz_convert("America/New_York").tz_localize(None)
            current["parsed_time"] = dt

        logger.info(f"Fetched API: {current.get('temperature')}°C at {current.get('parsed_time')}")
        return current

    except Exception as e:
        logger.warning(f"API request failed: {e}")
        return {}


@task(log_prints=True)
def extract_from_s3_parquet(s3_bucket: str, s3_prefix: str) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Reading from s3://{s3_bucket}/{s3_prefix}")

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    page_iterator = paginator.paginate(Bucket=s3_bucket, Prefix=s3_prefix)

    parquet_files = []
    for page in page_iterator:
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.parquet'):
                parquet_files.append(obj['Key'])

    logger.info(f"Found {len(parquet_files)} parquet files")

    all_dfs = []
    for file_key in parquet_files:
        year = month = None
        for part in file_key.split('/'):
            if part.startswith('year='):
                try:
                    year = int(part.split('=')[1])
                except:
                    year = None
            elif part.startswith('month='):
                try:
                    month = int(part.split('=')[1])
                except:
                    month = None

        obj = s3.get_object(Bucket=s3_bucket, Key=file_key)
        df = pd.read_parquet(BytesIO(obj['Body'].read()))

        if 'year' not in df.columns or df['year'].isna().all():
            df["year"] = year
        if 'month' not in df.columns or df['month'].isna().all():
            df["month"] = month

        all_dfs.append(df)

    if all_dfs:
        combined_df = pd.concat(all_dfs, ignore_index=True, sort=False)
    else:
        combined_df = pd.DataFrame()

    logger.info(f"Total rows loaded from S3: {len(combined_df)}")
    return combined_df


@task(log_prints=True)
def normalize_parquet_columns(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    logger.info(f"Normalizing columns for {len(df)} rows")

    if df.empty:
        logger.warning("Input df is empty")
        return df

    df = df.copy()

    for col in ["day", "year", "month"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    if "datatype" in df.columns and "value" in df.columns:
        mask = df["value"].notna() & df["datatype"].notna()
        if mask.any():
            legacy = df[mask].copy()
            legacy["element"] = legacy["datatype"]
            legacy["value_c"] = (legacy["value"] / 10 - 32) * 5 / 9
            df.loc[mask, "element"] = legacy.loc[mask.index, "element"]
            df.loc[mask, "value_c"] = legacy.loc[mask.index, "value_c"]

    wide_value_cols = [c for c in ["TMAX", "TMIN", "PRCP"] if c in df.columns]
    wide_rows_mask = df["value_c"].isna() & df[wide_value_cols].notna().any(axis=1) if wide_value_cols else pd.Series(False, index=df.index)

    long_rows_mask = df["element"].notna() & df["value_c"].notna()
    long_rows = df[long_rows_mask].copy()
    wide_rows = df[~long_rows_mask].copy()

    melted_rows = pd.DataFrame()
    if wide_value_cols:
        id_cols = [c for c in wide_rows.columns if c not in wide_value_cols]
        if not wide_rows.empty:
            try:
                melted = wide_rows.melt(id_vars=id_cols, value_vars=wide_value_cols,
                                        var_name='element', value_name='value_c')
                melted = melted[melted['value_c'].notna()].copy()
                melted_rows = melted
            except Exception as e:
                logger.warning(f"Failed to melt wide rows: {e}")
                melted_rows = pd.DataFrame()

    if not long_rows.empty and not melted_rows.empty:
        df_combined = pd.concat([long_rows, melted_rows], ignore_index=True, sort=False)
    elif not long_rows.empty:
        df_combined = long_rows.reset_index(drop=True)
    elif not melted_rows.empty:
        df_combined = melted_rows.reset_index(drop=True)
    else:
        df_combined = df.copy()

    if 'windspeed' not in df_combined.columns:
        df_combined['windspeed'] = np.nan

    for col in ['year', 'month', 'day']:
        if col not in df_combined.columns:
            df_combined[col] = pd.NA
        df_combined[col] = pd.to_numeric(df_combined[col], errors='coerce')

    if 'element' not in df_combined.columns:
        df_combined['element'] = pd.NA
    if 'value_c' not in df_combined.columns:
        df_combined['value_c'] = pd.NA

    logger.info(f"Normalized data rows: {len(df_combined)}")
    logger.info(f"Years in combined data: {sorted(pd.Series(df_combined['year'].dropna().unique()).tolist())}")
    logger.info(f"Unique elements: {pd.Series(df_combined['element'].dropna().unique()).tolist()}")
    return df_combined


@task(log_prints=True)
def compute_flags(df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()

    if df.empty:
        logger.warning("compute_flags received empty dataframe")
        return df

    df = df.copy()

    df["year"] = pd.to_numeric(df["year"], errors="coerce")
    df["month"] = pd.to_numeric(df["month"], errors="coerce")
    df["day"] = pd.to_numeric(df["day"], errors="coerce")
    df["value_c"] = pd.to_numeric(df["value_c"], errors="coerce")

    df_valid = df.dropna(subset=["year", "month", "day", "value_c", "element"]).copy()
    logger.info(f"Rows with valid year/month/day/value_c/element: {len(df_valid)}")

    if df_valid.empty:
        logger.warning("After filtering, no valid rows remain.")
        return df_valid

    df_valid["date"] = pd.to_datetime(
        df_valid[["year", "month", "day"]].astype(int),
        errors="coerce"
    )
    df_valid = df_valid.dropna(subset=["date"]).copy()

    threshold = 3

    tmax_df = df_valid[df_valid["element"] == "TMAX"].copy()

    if tmax_df.empty:
        logger.warning("No TMAX rows found for flag computation")
        return df_valid

    tmax_daily_mean = (
        tmax_df.groupby(["month", "day"])["value_c"]
        .mean()
        .reset_index()
        .rename(columns={"value_c": "daily_mean"})
    )
    tmax_df = tmax_df.merge(tmax_daily_mean, on=["month", "day"], how="left")

    tmax_monthly_mean = (
        tmax_df.groupby("month")["value_c"]
        .mean()
        .reset_index()
        .rename(columns={"value_c": "monthly_mean"})
    )
    tmax_df = tmax_df.merge(tmax_monthly_mean, on="month", how="left")

    tmax_df["daily_flag"] = np.where(
        tmax_df["value_c"] < tmax_df["daily_mean"] - threshold, "Below",
        np.where(tmax_df["value_c"] > tmax_df["daily_mean"] + threshold, "Above", "Average")
    )

    tmax_df["monthly_flag"] = np.where(
        tmax_df["value_c"] < tmax_df["monthly_mean"] - threshold, "Below",
        np.where(tmax_df["value_c"] > tmax_df["monthly_mean"] + threshold, "Above", "Average")
    )

    df_valid = df_valid.merge(
        tmax_df[["date", "daily_flag", "monthly_flag"]],
        on="date",
        how="left"
    )

    if 'windspeed' not in df_valid.columns:
        df_valid['windspeed'] = np.nan

    return df_valid


@task(log_prints=True)
def transform_api_data(api_data: dict) -> pd.DataFrame:
    logger = get_run_logger()
    if "temperature" not in api_data or "parsed_time" not in api_data:
        return pd.DataFrame()

    dt = api_data["parsed_time"]
    df = pd.DataFrame([{
        "year": dt.year,
        "month": dt.month,
        "day": dt.day,
        "date": dt,
        "value_c": api_data["temperature"],
        "TMAX": api_data["temperature"],
        "windspeed": api_data.get("windspeed", np.nan),
        "element": "TMAX",
        "source": "api"
    }])
    return df


@task(log_prints=True)
def compute_api_flags(historical_df: pd.DataFrame, api_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()
    if api_df.empty:
        logger.warning("API dataframe is empty, skipping flag computation")
        return api_df

    if historical_df.empty:
        logger.warning("Historical dataframe empty, cannot compute flags")
        api_df["daily_flag"] = np.nan
        api_df["monthly_flag"] = np.nan
        return api_df

    if 'date' not in api_df.columns:
        api_df['date'] = pd.to_datetime(api_df[['year', 'month', 'day']])

    threshold = 3

    daily_mean = (
        historical_df[historical_df['element']=='TMAX']
        .groupby(['month', 'day'])['value_c']
        .mean()
        .to_dict()
    )

    monthly_mean = (
        historical_df[historical_df['element']=='TMAX']
        .groupby(['month'])['value_c']
        .mean()
        .to_dict()
    )

    def daily_flag(row):
        avg = daily_mean.get((row['month'], row['day']), np.nan)
        if pd.isna(avg):
            return np.nan
        if row['TMAX'] > avg + threshold:
            return "Above"
        elif row['TMAX'] < avg - threshold:
            return "Below"
        else:
            return "Average"

    def monthly_flag(row):
        avg = monthly_mean.get(row['month'], np.nan)
        if pd.isna(avg):
            return np.nan
        if row['TMAX'] > avg + threshold:
            return "Above"
        elif row['TMAX'] < avg - threshold:
            return "Below"
        else:
            return "Average"

    api_df['daily_flag'] = api_df.apply(daily_flag, axis=1)
    api_df['monthly_flag'] = api_df.apply(monthly_flag, axis=1)
    return api_df


@task(log_prints=True)
def combine_and_pivot(file_df: pd.DataFrame, api_df: pd.DataFrame) -> pd.DataFrame:
    logger = get_run_logger()

    if file_df.empty:
        pivot_df = pd.DataFrame()
        logger.warning("file_df is empty")
    else:
        key_elements = ['TMAX', 'TMIN', 'PRCP']
        file_df = file_df[file_df['element'].isin(key_elements)].copy()

        pivot_df = file_df.pivot_table(
            index=["date", "year", "month", "day"],
            columns="element",
            values="value_c",
            aggfunc='mean'
        ).reset_index()

        for col in ["daily_flag", "monthly_flag", "windspeed"]:
            if col in file_df.columns:
                series = file_df.groupby("date")[col].first()
                pivot_df[col] = pivot_df['date'].map(series)

    if not api_df.empty:
        pivot_df = pd.concat([pivot_df, api_df], ignore_index=True, sort=False)

    logger.info(f"Final combined dataframe rows: {len(pivot_df)}")
    return pivot_df


@task(log_prints=True)
def upload_to_s3_csv(df: pd.DataFrame, bucket: str, key: str):
    logger = get_run_logger()
    from io import StringIO

    s3 = boto3.client("s3")
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
    logger.info(f"Uploaded CSV to s3://{bucket}/{key}")


@flow(name="boston-weather-pipeline", log_prints=True)
def boston_weather_pipeline(
    s3_bucket="weather-etl-emily-2025",
    s3_parquet_prefix="processed/raw/",
    s3_output_bucket="weather-etl-emily-2025",
    s3_output_key="combined/boston_weather_combined.csv"
):
    api_data = extract_from_api()
    boston_df = extract_from_s3_parquet(s3_bucket, s3_parquet_prefix)
    normalized_df = normalize_parquet_columns(boston_df)
    flagged_df = compute_flags(normalized_df)
    processed_api = transform_api_data(api_data)
    processed_api = compute_api_flags(flagged_df, processed_api)
    final_df = combine_and_pivot(flagged_df, processed_api)
    upload_to_s3_csv(final_df, s3_output_bucket, s3_output_key)

    return {"records": len(final_df), "csv_s3_path": f"s3://{s3_output_bucket}/{s3_output_key}"}

if __name__ == "__main__":
    # Serve the Boston weather pipeline on a daily schedule
    boston_weather_pipeline.serve(
        name="boston-weather-deployment",
        interval=86400,  # run every 24 hours (in seconds)
        tags=["weather", "boston", "etl", "production"],
        description="Boston weather ETL pipeline"
    )