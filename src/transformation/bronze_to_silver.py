"""
Bronze to Silver Transformation

Transforms raw JSON stock data from the Bronze layer into cleaned,
typed Parquet files in the Silver layer.
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from io import BytesIO

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.minio_client import (
    get_datalake,
    BRONZE_BUCKET,
    SILVER_BUCKET
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BronzeToSilverTransformer:
    """
    Transforms raw stock data from Bronze to Silver layer.
    
    Applies data cleaning, type casting, and schema validation
    before storing as Parquet files.
    """
    
    # Expected schema for stock data
    STOCK_SCHEMA = pa.schema([
        ('symbol', pa.string()),
        ('date', pa.date32()),
        ('open', pa.float64()),
        ('high', pa.float64()),
        ('low', pa.float64()),
        ('close', pa.float64()),
        ('adjusted_close', pa.float64()),
        ('volume', pa.int64()),
        ('dividend_amount', pa.float64()),
        ('split_coefficient', pa.float64()),
    ])
    
    def __init__(self):
        """Initialize the transformer with data lake connection."""
        self.datalake = get_datalake()
    
    def transform_time_series(
        self,
        symbol: str,
        time_series: Dict[str, Dict[str, str]]
    ) -> pd.DataFrame:
        """
        Transform raw time series data into a clean DataFrame.
        
        Args:
            symbol: Stock ticker symbol
            time_series: Raw time series data from Alpha Vantage
            
        Returns:
            Cleaned pandas DataFrame
        """
        records = []
        
        for date_str, values in time_series.items():
            try:
                close_price = float(values.get('4. close', 0))
                record = {
                    'symbol': symbol,
                    'date': pd.to_datetime(date_str).date(),
                    'open': float(values.get('1. open', 0)),
                    'high': float(values.get('2. high', 0)),
                    'low': float(values.get('3. low', 0)),
                    'close': close_price,
                    # Use close as adjusted_close for non-adjusted API
                    'adjusted_close': float(values.get('5. adjusted close', close_price)),
                    # Volume is at position 5 for non-adjusted, position 6 for adjusted
                    'volume': int(float(values.get('5. volume', values.get('6. volume', 0)))),
                    'dividend_amount': float(values.get('7. dividend amount', 0)),
                    'split_coefficient': float(values.get('8. split coefficient', 1)),
                }
                records.append(record)
            except (ValueError, TypeError) as e:
                logger.warning(f"Skipping invalid record for {symbol} on {date_str}: {e}")
                continue
        
        if not records:
            logger.warning(f"No valid records found for {symbol}")
            return pd.DataFrame()
        
        df = pd.DataFrame(records)
        
        # Sort by date (oldest first)
        df = df.sort_values('date').reset_index(drop=True)
        
        # Data quality checks
        df = self._apply_quality_checks(df)
        
        return df
    
    def _apply_quality_checks(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply data quality checks and fixes."""
        
        # Remove rows with zero prices (likely bad data)
        df = df[df['close'] > 0].copy()
        
        # Ensure high >= low
        invalid_range = df['high'] < df['low']
        if invalid_range.any():
            logger.warning(f"Found {invalid_range.sum()} rows with high < low, fixing...")
            df.loc[invalid_range, ['high', 'low']] = df.loc[invalid_range, ['low', 'high']].values
        
        # Ensure volume is non-negative
        df['volume'] = df['volume'].clip(lower=0)
        
        # Fill missing adjusted_close with close
        df['adjusted_close'] = df['adjusted_close'].fillna(df['close'])
        
        return df
    
    def dataframe_to_parquet_bytes(self, df: pd.DataFrame) -> bytes:
        """Convert DataFrame to Parquet bytes."""
        buffer = BytesIO()
        table = pa.Table.from_pandas(df, schema=self.STOCK_SCHEMA, preserve_index=False)
        pq.write_table(table, buffer)
        return buffer.getvalue()
    
    def process_bronze_file(
        self,
        symbol: str,
        date: str
    ) -> Optional[str]:
        """
        Process a single Bronze file and store in Silver layer.
        
        Args:
            symbol: Stock ticker symbol
            date: Date string of the Bronze file
            
        Returns:
            Path to Silver file or None if failed
        """
        object_name = f"stocks/{symbol}/{date}.json"
        
        try:
            # Download from Bronze
            raw_data = self.datalake.download_json(BRONZE_BUCKET, object_name)
            
            # Extract time series
            time_series = raw_data.get('time_series', {})
            
            if not time_series:
                logger.warning(f"No time series data in {object_name}")
                return None
            
            # Transform to DataFrame
            df = self.transform_time_series(symbol, time_series)
            
            if df.empty:
                logger.warning(f"Empty DataFrame after transformation for {symbol}")
                return None
            
            # Convert to Parquet
            parquet_bytes = self.dataframe_to_parquet_bytes(df)
            
            # Upload to Silver
            path = self.datalake.upload_to_silver(symbol, date, parquet_bytes)
            
            logger.info(f"Transformed {len(df)} records for {symbol} to Silver layer")
            return path
            
        except Exception as e:
            logger.error(f"Error processing {symbol}/{date}: {e}")
            return None
    
    def process_all_bronze_files(
        self,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, List[str]]:
        """
        Process all Bronze files and transform to Silver.
        
        Args:
            symbols: Optional list of symbols to filter
            
        Returns:
            Dictionary mapping symbols to list of processed file paths
        """
        results = {}
        
        # Get all Bronze files
        bronze_files = self.datalake.get_bronze_files()
        
        for object_name in bronze_files:
            # Parse symbol and date from object name
            # Format: stocks/{symbol}/{date}.json
            parts = object_name.split('/')
            if len(parts) < 3:
                continue
            
            symbol = parts[1]
            date = parts[2].replace('.json', '')
            
            # Filter by symbols if specified
            if symbols and symbol not in symbols:
                continue
            
            # Process file
            path = self.process_bronze_file(symbol, date)
            
            if symbol not in results:
                results[symbol] = []
            
            if path:
                results[symbol].append(path)
        
        return results


def transform_bronze_to_silver(
    symbols: Optional[List[str]] = None
) -> Dict[str, List[str]]:
    """
    Main entry point for Bronze to Silver transformation.
    
    This function is called by the Airflow DAG to transform
    raw data into cleaned Parquet files.
    
    Args:
        symbols: Optional list of symbols to process
        
    Returns:
        Dictionary mapping symbols to processed file paths
    """
    transformer = BronzeToSilverTransformer()
    results = transformer.process_all_bronze_files(symbols=symbols)
    
    # Log summary
    total_files = sum(len(files) for files in results.values())
    logger.info(f"Bronze to Silver complete: {total_files} files processed for {len(results)} symbols")
    
    return results


if __name__ == "__main__":
    # Test the transformer locally
    results = transform_bronze_to_silver()
    
    for symbol, paths in results.items():
        print(f"{symbol}: {len(paths)} files processed")
        for path in paths:
            print(f"  - {path}")
