"""
Silver to Gold Transformation

Calculates KPIs and aggregations from Silver layer data and stores
them in the Gold layer of the data lake.
"""

import os
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any
from io import BytesIO

import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ingestion.minio_client import (
    get_datalake,
    SILVER_BUCKET,
    GOLD_BUCKET
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class SilverToGoldTransformer:
    """
    Transforms Silver layer data into Gold layer KPIs.
    
    Calculates various metrics including:
    - Daily returns
    - Moving averages (7-day, 30-day)
    - Volatility (rolling standard deviation)
    - Volume trends
    - Price range analysis
    """
    
    # KPI schema for Parquet files
    KPI_SCHEMA = pa.schema([
        ('symbol', pa.string()),
        ('date', pa.date32()),
        ('close', pa.float64()),
        ('daily_return', pa.float64()),
        ('daily_return_pct', pa.float64()),
        ('sma_7', pa.float64()),
        ('sma_30', pa.float64()),
        ('ema_7', pa.float64()),
        ('ema_30', pa.float64()),
        ('volatility_7d', pa.float64()),
        ('volatility_30d', pa.float64()),
        ('avg_volume_7d', pa.float64()),
        ('avg_volume_30d', pa.float64()),
        ('price_range', pa.float64()),
        ('price_range_pct', pa.float64()),
        ('volume_change_pct', pa.float64()),
        ('rsi_14', pa.float64()),
    ])
    
    def __init__(self):
        """Initialize the transformer with data lake connection."""
        self.datalake = get_datalake()
    
    def load_silver_data(self, symbol: str) -> pd.DataFrame:
        """
        Load all Silver layer data for a symbol.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Combined DataFrame of all Silver data for the symbol
        """
        silver_files = self.datalake.get_silver_files(symbol)
        
        if not silver_files:
            logger.warning(f"No Silver files found for {symbol}")
            return pd.DataFrame()
        
        dfs = []
        for object_name in silver_files:
            try:
                parquet_bytes = self.datalake.download_parquet(SILVER_BUCKET, object_name)
                df = pd.read_parquet(BytesIO(parquet_bytes))
                dfs.append(df)
            except Exception as e:
                logger.warning(f"Error loading {object_name}: {e}")
                continue
        
        if not dfs:
            return pd.DataFrame()
        
        # Combine all data
        combined = pd.concat(dfs, ignore_index=True)
        
        # Remove duplicates and sort by date
        combined = combined.drop_duplicates(subset=['symbol', 'date'])
        combined = combined.sort_values('date').reset_index(drop=True)
        
        return combined
    
    def calculate_kpis(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Calculate all KPIs from the Silver layer data.
        
        Args:
            df: Silver layer DataFrame
            
        Returns:
            DataFrame with calculated KPIs
        """
        if df.empty:
            return df
        
        # Ensure data is sorted by date
        df = df.sort_values('date').reset_index(drop=True)
        
        # Daily returns
        df['daily_return'] = df['adjusted_close'].diff()
        df['daily_return_pct'] = df['adjusted_close'].pct_change() * 100
        
        # Simple Moving Averages
        df['sma_7'] = df['adjusted_close'].rolling(window=7, min_periods=1).mean()
        df['sma_30'] = df['adjusted_close'].rolling(window=30, min_periods=1).mean()
        
        # Exponential Moving Averages
        df['ema_7'] = df['adjusted_close'].ewm(span=7, adjust=False).mean()
        df['ema_30'] = df['adjusted_close'].ewm(span=30, adjust=False).mean()
        
        # Volatility (rolling standard deviation of returns)
        df['volatility_7d'] = df['daily_return_pct'].rolling(window=7, min_periods=1).std()
        df['volatility_30d'] = df['daily_return_pct'].rolling(window=30, min_periods=1).std()
        
        # Volume trends
        df['avg_volume_7d'] = df['volume'].rolling(window=7, min_periods=1).mean()
        df['avg_volume_30d'] = df['volume'].rolling(window=30, min_periods=1).mean()
        df['volume_change_pct'] = df['volume'].pct_change() * 100
        
        # Price range (High - Low)
        df['price_range'] = df['high'] - df['low']
        df['price_range_pct'] = (df['price_range'] / df['low']) * 100
        
        # RSI (Relative Strength Index) - 14 period
        df['rsi_14'] = self._calculate_rsi(df['adjusted_close'], period=14)
        
        # Fill NaN values with 0 for initial periods
        df = df.fillna(0)
        
        # Select relevant columns for Gold layer
        kpi_columns = [
            'symbol', 'date', 'close',
            'daily_return', 'daily_return_pct',
            'sma_7', 'sma_30', 'ema_7', 'ema_30',
            'volatility_7d', 'volatility_30d',
            'avg_volume_7d', 'avg_volume_30d',
            'price_range', 'price_range_pct',
            'volume_change_pct', 'rsi_14'
        ]
        
        # Rename close to match schema
        df['close'] = df['adjusted_close']
        
        return df[kpi_columns]
    
    def _calculate_rsi(self, prices: pd.Series, period: int = 14) -> pd.Series:
        """Calculate RSI (Relative Strength Index)."""
        delta = prices.diff()
        
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def dataframe_to_parquet_bytes(self, df: pd.DataFrame) -> bytes:
        """Convert DataFrame to Parquet bytes."""
        buffer = BytesIO()
        table = pa.Table.from_pandas(df, schema=self.KPI_SCHEMA, preserve_index=False)
        pq.write_table(table, buffer)
        return buffer.getvalue()
    
    def process_symbol(self, symbol: str) -> Optional[str]:
        """
        Process Silver data for a symbol and create Gold layer KPIs.
        
        Args:
            symbol: Stock ticker symbol
            
        Returns:
            Path to Gold file or None if failed
        """
        try:
            # Load Silver data
            df = self.load_silver_data(symbol)
            
            if df.empty:
                logger.warning(f"No data to process for {symbol}")
                return None
            
            # Calculate KPIs
            kpi_df = self.calculate_kpis(df)
            
            if kpi_df.empty:
                logger.warning(f"No KPIs calculated for {symbol}")
                return None
            
            # Convert to Parquet
            parquet_bytes = self.dataframe_to_parquet_bytes(kpi_df)
            
            # Upload to Gold layer
            date_str = datetime.utcnow().strftime("%Y-%m-%d")
            path = self.datalake.upload_to_gold(symbol, date_str, parquet_bytes, kpi_type="daily")
            
            logger.info(f"Calculated KPIs for {symbol}: {len(kpi_df)} records")
            return path
            
        except Exception as e:
            logger.error(f"Error processing {symbol}: {e}")
            return None
    
    def process_all_symbols(
        self,
        symbols: Optional[List[str]] = None
    ) -> Dict[str, Optional[str]]:
        """
        Process all symbols from Silver layer.
        
        Args:
            symbols: Optional list of symbols to process
            
        Returns:
            Dictionary mapping symbols to Gold file paths
        """
        results = {}
        
        # Get all symbols from Silver layer
        silver_files = self.datalake.get_silver_files()
        available_symbols = set()
        
        for obj_name in silver_files:
            parts = obj_name.split('/')
            if len(parts) >= 2:
                available_symbols.add(parts[1])
        
        # Filter by specified symbols if provided
        if symbols:
            available_symbols = available_symbols.intersection(set(symbols))
        
        for symbol in available_symbols:
            path = self.process_symbol(symbol)
            results[symbol] = path
        
        return results


def transform_silver_to_gold(
    symbols: Optional[List[str]] = None
) -> Dict[str, Optional[str]]:
    """
    Main entry point for Silver to Gold transformation.
    
    This function is called by the Airflow DAG to calculate
    KPIs and aggregations for the Gold layer.
    
    Args:
        symbols: Optional list of symbols to process
        
    Returns:
        Dictionary mapping symbols to Gold file paths
    """
    transformer = SilverToGoldTransformer()
    results = transformer.process_all_symbols(symbols=symbols)
    
    # Log summary
    success_count = sum(1 for path in results.values() if path is not None)
    logger.info(f"Silver to Gold complete: {success_count}/{len(results)} symbols processed")
    
    return results


if __name__ == "__main__":
    # Test the transformer locally
    results = transform_silver_to_gold()
    
    for symbol, path in results.items():
        if path:
            print(f"✓ {symbol}: {path}")
        else:
            print(f"✗ {symbol}: Failed")
