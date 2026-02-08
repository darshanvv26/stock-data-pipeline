# Stock Data Pipeline

End-to-end data pipeline for stock market analysis using **Medallion Architecture** with MinIO data lake, Apache Airflow orchestration, PostgreSQL data warehouse, and Streamlit dashboard.

## Architecture

```
┌─────────────────┐     ┌─────────────────────────────────────────┐     ┌──────────────┐
│  Alpha Vantage  │────▶│            MinIO Data Lake              │────▶│  PostgreSQL  │
│      API        │     │  Bronze → Silver → Gold (Medallion)     │     │  Warehouse   │
└─────────────────┘     └─────────────────────────────────────────┘     └──────────────┘
                                         │                                      │
                                         │        ┌─────────────────┐           │
                                         └───────▶│    Streamlit    │◀──────────┘
                                                  │    Dashboard    │
                                                  └─────────────────┘
                        ┌─────────────────────────────────────────────────────────────┐
                        │                   Apache Airflow                            │
                        │    (fetch_data → transform_silver → calculate_kpis → load)  │
                        └─────────────────────────────────────────────────────────────┘
```

## Components

| Component | Technology | Description |
|-----------|------------|-------------|
| **Data Source** | Alpha Vantage API | Free stock market data (25 requests/day) |
| **Data Lake** | MinIO | S3-compatible object storage |
| **Bronze Layer** | JSON files | Raw API responses |
| **Silver Layer** | Parquet files | Cleaned, validated data |
| **Gold Layer** | Parquet + PostgreSQL | KPIs and aggregations |
| **Orchestration** | Apache Airflow | DAG-based pipeline scheduling |
| **Dashboard** | Streamlit | Interactive visualization |

## Quick Start

### Prerequisites

- Docker & Docker Compose
- Alpha Vantage API Key (free at [alphavantage.co](https://www.alphavantage.co/support/#api-key))

### 1. Clone and Configure

```bash
cd stock-data-pipeline

# Copy environment file
cp .env.example .env

# Edit .env and add your Alpha Vantage API key
# ALPHA_VANTAGE_API_KEY=your_key_here
```

### 2. Start Infrastructure

```bash
# Start all services
docker-compose up -d

# Check status
docker-compose ps
```

### 3. Access Services

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow** | [http://localhost:8080](http://localhost:8080) | admin / admin |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | minioadmin / minioadmin |
| **Dashboard** | [http://localhost:8501](http://localhost:8501) | - |
| **PostgreSQL** | localhost:5432 | airflow / airflow |

### 4. Run the Pipeline

1. Open Airflow at [http://localhost:8080](http://localhost:8080)
2. Navigate to DAGs → `stock_data_pipeline`
3. Click the "Play" button to trigger the DAG
4. Monitor task progress (all tasks should turn green)

### 5. View Results

1. **MinIO Console**: Check bronze/silver/gold buckets for data files
2. **Dashboard**: View stock charts and KPIs at [http://localhost:8501](http://localhost:8501)

## Medallion Architecture

### Bronze Layer (Raw Data)
- Location: `bronze/stocks/{symbol}/{date}.json`
- Content: Raw API response with metadata
- Format: JSON

### Silver Layer (Cleaned Data)
- Location: `silver/stocks/{symbol}/{date}.parquet`
- Content: Validated, typed stock prices
- Schema: symbol, date, open, high, low, close, adjusted_close, volume

### Gold Layer (KPIs)
- Location: `gold/kpis/daily/{symbol}/{date}.parquet`
- Metrics calculated:
  - **Daily Returns**: Percentage change from previous close
  - **Moving Averages**: 7-day and 30-day SMA/EMA
  - **Volatility**: Rolling standard deviation (7d, 30d)
  - **RSI**: 14-period Relative Strength Index
  - **Volume Trends**: Average volume over periods

## KPIs Calculated

| KPI | Description |
|-----|-------------|
| `daily_return` | Day-over-day price change |
| `daily_return_pct` | Percentage return |
| `sma_7` / `sma_30` | Simple Moving Average |
| `ema_7` / `ema_30` | Exponential Moving Average |
| `volatility_7d` / `volatility_30d` | Rolling volatility |
| `rsi_14` | Relative Strength Index |
| `avg_volume_7d` / `avg_volume_30d` | Average trading volume |
| `price_range` | Daily high-low spread |

## Configuration

### Stock Symbols

Edit `dags/stock_pipeline_dag.py` to change tracked stocks:

```python
STOCK_SYMBOLS = ['AAPL', 'GOOGL', 'MSFT', 'AMZN']
```

### Schedule

Default schedule: Daily at 6 PM UTC (Mon-Fri, after market close)

```python
schedule_interval='0 18 * * 1-5'
```

## Project Structure

```
stock-data-pipeline/
├── docker-compose.yml        # Infrastructure configuration
├── .env                      # Environment variables
├── requirements.txt          # Python dependencies
├── dags/
│   └── stock_pipeline_dag.py # Airflow DAG
├── src/
│   ├── ingestion/
│   │   ├── minio_client.py   # MinIO data lake client
│   │   └── stock_fetcher.py  # Alpha Vantage fetcher
│   ├── transformation/
│   │   ├── bronze_to_silver.py
│   │   └── silver_to_gold.py
│   └── db/
│       └── warehouse.py      # PostgreSQL warehouse
├── dashboard/
│   ├── app.py                # Streamlit dashboard
│   └── Dockerfile
└── scripts/
    └── init_db.sql           # Database initialization
```

## Development

### Local Testing

```bash
# Install dependencies
pip install -r requirements.txt

# Test ingestion (uses demo API key with IBM only)
python -m src.ingestion.stock_fetcher

# Test transformation
python -m src.transformation.bronze_to_silver
python -m src.transformation.silver_to_gold
```

### Rebuilding Services

```bash
# Rebuild and restart
docker-compose down
docker-compose build --no-cache
docker-compose up -d
```

## Troubleshooting

### No Data in Dashboard
1. Check if DAG ran successfully in Airflow
2. Verify MinIO buckets have data
3. Check Airflow task logs for errors

### API Rate Limit
- Free tier: 25 requests/day
- Use `demo` key for testing (IBM stock only)
- Reduce symbol count or use caching

### Database Connection Issues
```bash
# Check PostgreSQL logs
docker logs postgres

# Connect directly
docker exec -it postgres psql -U airflow -d stockdb
```
