"""
Stock Data Pipeline DAG

Apache Airflow DAG that orchestrates the end-to-end stock data pipeline:
1. Fetch stock data from Alpha Vantage API (Bronze layer)
2. Transform raw data to cleaned Parquet (Silver layer)
3. Calculate KPIs and aggregations (Gold layer)
4. Load KPIs to PostgreSQL data warehouse

Schedule: Daily at 6 PM UTC (after market close)
"""

from datetime import datetime, timedelta
from typing import List, Dict, Any
from io import BytesIO

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

import pandas as pd

# Import pipeline modules
from ingestion.stock_fetcher import ingest_stock_data
from ingestion.minio_client import get_datalake, GOLD_BUCKET
from transformation.bronze_to_silver import transform_bronze_to_silver
from transformation.silver_to_gold import transform_silver_to_gold
from db.warehouse import get_warehouse


# Default arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Stock symbols to track
STOCK_SYMBOLS = ['AAPL', 'GOOG', 'MSFT', 'AMZN']


def task_fetch_stock_data(**context) -> Dict[str, Any]:
    """
    Task: Fetch stock data from Alpha Vantage API.
    
    Stores raw JSON data in the Bronze layer of MinIO.
    """
    results = ingest_stock_data(symbols=STOCK_SYMBOLS)
    
    # Push results to XCom for downstream tasks
    context['ti'].xcom_push(key='bronze_paths', value=results)
    
    return {
        'status': 'success',
        'symbols_processed': len([p for p in results.values() if p]),
        'total_symbols': len(results)
    }


def task_transform_to_silver(**context) -> Dict[str, Any]:
    """
    Task: Transform Bronze layer data to Silver layer.
    
    Cleans and validates data, converts to Parquet format.
    """
    results = transform_bronze_to_silver(symbols=STOCK_SYMBOLS)
    
    # Push results to XCom
    context['ti'].xcom_push(key='silver_paths', value=results)
    
    total_files = sum(len(files) for files in results.values())
    
    return {
        'status': 'success',
        'symbols_processed': len(results),
        'files_created': total_files
    }


def task_calculate_kpis(**context) -> Dict[str, Any]:
    """
    Task: Calculate KPIs from Silver layer data.
    
    Computes daily returns, moving averages, volatility, RSI, etc.
    Stores results in Gold layer.
    """
    results = transform_silver_to_gold(symbols=STOCK_SYMBOLS)
    
    # Push results to XCom
    context['ti'].xcom_push(key='gold_paths', value=results)
    
    return {
        'status': 'success',
        'symbols_processed': len([p for p in results.values() if p]),
        'total_symbols': len(results)
    }


def task_load_to_warehouse(**context) -> Dict[str, Any]:
    """
    Task: Load Gold layer KPIs to PostgreSQL warehouse.
    
    Makes data available for dashboard queries.
    """
    datalake = get_datalake()
    warehouse = get_warehouse()
    
    total_rows = 0
    
    # Get all Gold layer files
    gold_files = datalake.get_gold_files(kpi_type='daily')
    
    for object_name in gold_files:
        try:
            # Download Parquet from Gold layer
            parquet_bytes = datalake.download_parquet(GOLD_BUCKET, object_name)
            df = pd.read_parquet(BytesIO(parquet_bytes))
            
            # Insert into warehouse
            rows = warehouse.insert_stock_kpis(df)
            total_rows += rows
            
        except Exception as e:
            print(f"Error loading {object_name}: {e}")
            continue
    
    return {
        'status': 'success',
        'files_processed': len(gold_files),
        'rows_inserted': total_rows
    }


# Define the DAG
with DAG(
    dag_id='stock_data_pipeline',
    default_args=default_args,
    description='End-to-end stock data pipeline with medallion architecture',
    schedule_interval='0 18 * * 1-5',  # 6 PM UTC, Mon-Fri (after market close)
    start_date=days_ago(1),
    catchup=False,
    tags=['stocks', 'data-pipeline', 'medallion'],
) as dag:
    
    # Start marker
    start = EmptyOperator(task_id='start')
    
    # Task 1: Fetch stock data (Bronze layer)
    fetch_data = PythonOperator(
        task_id='fetch_stock_data',
        python_callable=task_fetch_stock_data,
        provide_context=True,
    )
    
    # Task 2: Transform to Silver layer
    transform_silver = PythonOperator(
        task_id='transform_to_silver',
        python_callable=task_transform_to_silver,
        provide_context=True,
    )
    
    # Task 3: Calculate KPIs (Gold layer)
    calculate_kpis = PythonOperator(
        task_id='calculate_kpis',
        python_callable=task_calculate_kpis,
        provide_context=True,
    )
    
    # Task 4: Load to warehouse
    load_warehouse = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=task_load_to_warehouse,
        provide_context=True,
    )
    
    # End marker
    end = EmptyOperator(task_id='end')
    
    # Define task dependencies (pipeline flow)
    start >> fetch_data >> transform_silver >> calculate_kpis >> load_warehouse >> end
