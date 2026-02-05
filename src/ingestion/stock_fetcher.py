"""
Stock Data Fetcher

Fetches stock market data from Alpha Vantage API and stores it in the
Bronze layer of the data lake.
"""

import os
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import requests

from ingestion.minio_client import get_datalake, BRONZE_BUCKET

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Alpha Vantage API configuration
ALPHA_VANTAGE_BASE_URL = "https://www.alphavantage.co/query"
DEFAULT_SYMBOLS = ["AAPL", "GOOG", "MSFT", "AMZN"]


class StockDataFetcher:
    """
    Fetches stock data from Alpha Vantage API.
    
    Supports daily time series data with configurable output size
    and multiple stock symbols.
    """
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the stock data fetcher.
        
        Args:
            api_key: Alpha Vantage API key (default from env)
        """
        self.api_key = api_key or os.getenv("ALPHA_VANTAGE_API_KEY", "demo")
        self.datalake = get_datalake()
        
        if self.api_key == "demo":
            logger.warning(
                "Using demo API key. Limited to IBM stock only. "
                "Get a free key at https://www.alphavantage.co/support/#api-key"
            )
    
    def fetch_daily_data(
        self,
        symbol: str,
        outputsize: str = "compact"
    ) -> Optional[Dict[str, Any]]:
        """
        Fetch daily time series data for a stock symbol.
        
        Args:
            symbol: Stock ticker symbol (e.g., 'AAPL')
            outputsize: 'compact' (100 days) or 'full' (20+ years)
            
        Returns:
            Raw API response data or None if failed
        """
        params = {
            "function": "TIME_SERIES_DAILY",
            "symbol": symbol,
            "outputsize": outputsize,
            "apikey": self.api_key
        }
        
        try:
            logger.info(f"Fetching daily data for {symbol}...")
            response = requests.get(ALPHA_VANTAGE_BASE_URL, params=params, timeout=30)
            response.raise_for_status()
            
            data = response.json()
            
            # Check for API errors
            if "Error Message" in data:
                logger.error(f"API Error for {symbol}: {data['Error Message']}")
                return None
            
            if "Note" in data:
                logger.warning(f"API Rate Limit: {data['Note']}")
                return None
            
            if "Time Series (Daily)" not in data:
                logger.error(f"Unexpected response format for {symbol}")
                return None
            
            logger.info(f"Successfully fetched data for {symbol}")
            return data
            
        except requests.RequestException as e:
            logger.error(f"Request failed for {symbol}: {e}")
            return None
    
    def fetch_and_store(
        self,
        symbol: str,
        outputsize: str = "compact"
    ) -> Optional[str]:
        """
        Fetch stock data and store in Bronze layer.
        
        Args:
            symbol: Stock ticker symbol
            outputsize: 'compact' or 'full'
            
        Returns:
            Path to stored file or None if failed
        """
        data = self.fetch_daily_data(symbol, outputsize)
        
        if data is None:
            return None
        
        # Add metadata to the raw data
        enriched_data = {
            "metadata": {
                "symbol": symbol,
                "fetched_at": datetime.utcnow().isoformat(),
                "source": "alpha_vantage",
                "api_info": data.get("Meta Data", {})
            },
            "time_series": data.get("Time Series (Daily)", {})
        }
        
        # Store in Bronze layer with date partitioning
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        path = self.datalake.upload_to_bronze(symbol, date_str, enriched_data)
        
        return path
    
    def fetch_multiple_symbols(
        self,
        symbols: Optional[List[str]] = None,
        outputsize: str = "compact"
    ) -> Dict[str, Optional[str]]:
        """
        Fetch data for multiple stock symbols.
        
        Args:
            symbols: List of stock symbols (default: DEFAULT_SYMBOLS)
            outputsize: 'compact' or 'full'
            
        Returns:
            Dictionary mapping symbols to their stored file paths
        """
        symbols = symbols or DEFAULT_SYMBOLS
        results = {}
        
        for symbol in symbols:
            logger.info(f"Processing symbol: {symbol}")
            path = self.fetch_and_store(symbol, outputsize)
            results[symbol] = path
            
            if path:
                logger.info(f"Stored {symbol} data at: {path}")
            else:
                logger.warning(f"Failed to fetch/store data for {symbol}")
        
        return results


def ingest_stock_data(
    symbols: Optional[List[str]] = None,
    api_key: Optional[str] = None
) -> Dict[str, Optional[str]]:
    """
    Main entry point for stock data ingestion.
    
    This function is called by the Airflow DAG to ingest stock data
    into the Bronze layer of the data lake.
    
    Args:
        symbols: List of stock symbols to fetch
        api_key: Alpha Vantage API key
        
    Returns:
        Dictionary mapping symbols to stored file paths
    """
    fetcher = StockDataFetcher(api_key=api_key)
    
    # Ensure buckets exist
    fetcher.datalake.ensure_buckets_exist()
    
    # Fetch and store data
    results = fetcher.fetch_multiple_symbols(symbols=symbols)
    
    # Log summary
    success_count = sum(1 for path in results.values() if path is not None)
    logger.info(f"Ingestion complete: {success_count}/{len(results)} symbols successful")
    
    return results


if __name__ == "__main__":
    # Test the fetcher locally
    import sys
    
    # Use demo key for testing (only works with IBM)
    if len(sys.argv) > 1:
        symbols = sys.argv[1:]
    else:
        symbols = ["IBM"]  # Demo API only supports IBM
    
    results = ingest_stock_data(symbols=symbols)
    
    for symbol, path in results.items():
        if path:
            print(f"✓ {symbol}: {path}")
        else:
            print(f"✗ {symbol}: Failed")
