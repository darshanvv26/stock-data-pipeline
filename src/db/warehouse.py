"""
Data Warehouse Module

Handles PostgreSQL database operations for storing Gold layer data
and providing data access for the dashboard.
"""

import os
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any
from contextlib import contextmanager

import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DataWarehouse:
    """
    PostgreSQL data warehouse for Gold layer data.
    
    Provides methods for:
    - Storing stock prices (Silver layer backup)
    - Storing KPIs (Gold layer)
    - Querying data for dashboard
    """
    
    def __init__(
        self,
        host: Optional[str] = None,
        port: Optional[int] = None,
        database: Optional[str] = None,
        user: Optional[str] = None,
        password: Optional[str] = None
    ):
        """
        Initialize database connection parameters.
        
        Args:
            host: Database host
            port: Database port
            database: Database name
            user: Database user
            password: Database password
        """
        self.host = host or os.getenv("WAREHOUSE_HOST", "localhost")
        self.port = port or int(os.getenv("WAREHOUSE_PORT", "5432"))
        self.database = database or os.getenv("WAREHOUSE_DB", "stockdb")
        self.user = user or os.getenv("WAREHOUSE_USER", "airflow")
        self.password = password or os.getenv("WAREHOUSE_PASSWORD", "airflow")
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                database=self.database,
                user=self.user,
                password=self.password
            )
            yield conn
            conn.commit()
        except Exception as e:
            if conn:
                conn.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def insert_stock_prices(self, df: pd.DataFrame) -> int:
        """
        Insert or update stock prices in the warehouse.
        
        Args:
            df: DataFrame with stock price data
            
        Returns:
            Number of rows inserted/updated
        """
        if df.empty:
            return 0
        
        insert_query = """
            INSERT INTO stock_prices 
            (symbol, date, open_price, high_price, low_price, close_price, 
             adjusted_close, volume)
            VALUES %s
            ON CONFLICT (symbol, date) 
            DO UPDATE SET
                open_price = EXCLUDED.open_price,
                high_price = EXCLUDED.high_price,
                low_price = EXCLUDED.low_price,
                close_price = EXCLUDED.close_price,
                adjusted_close = EXCLUDED.adjusted_close,
                volume = EXCLUDED.volume
        """
        
        # Prepare data tuples
        records = [
            (
                row['symbol'],
                row['date'],
                row.get('open', row.get('open_price')),
                row.get('high', row.get('high_price')),
                row.get('low', row.get('low_price')),
                row.get('close', row.get('close_price')),
                row.get('adjusted_close'),
                row.get('volume')
            )
            for _, row in df.iterrows()
        ]
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, records)
                count = len(records)
        
        logger.info(f"Inserted/updated {count} stock price records")
        return count
    
    def insert_stock_kpis(self, df: pd.DataFrame) -> int:
        """
        Insert or update stock KPIs in the warehouse.
        
        Args:
            df: DataFrame with KPI data
            
        Returns:
            Number of rows inserted/updated
        """
        if df.empty:
            return 0
        
        insert_query = """
            INSERT INTO stock_kpis 
            (symbol, date, daily_return, sma_7, sma_30, volatility_7d, 
             volatility_30d, avg_volume_7d, price_range, price_range_pct)
            VALUES %s
            ON CONFLICT (symbol, date) 
            DO UPDATE SET
                daily_return = EXCLUDED.daily_return,
                sma_7 = EXCLUDED.sma_7,
                sma_30 = EXCLUDED.sma_30,
                volatility_7d = EXCLUDED.volatility_7d,
                volatility_30d = EXCLUDED.volatility_30d,
                avg_volume_7d = EXCLUDED.avg_volume_7d,
                price_range = EXCLUDED.price_range,
                price_range_pct = EXCLUDED.price_range_pct
        """
        
        # Prepare data tuples
        records = [
            (
                row['symbol'],
                row['date'],
                row.get('daily_return_pct', row.get('daily_return', 0)),
                row.get('sma_7', 0),
                row.get('sma_30', 0),
                row.get('volatility_7d', 0),
                row.get('volatility_30d', 0),
                row.get('avg_volume_7d', 0),
                row.get('price_range', 0),
                row.get('price_range_pct', 0)
            )
            for _, row in df.iterrows()
        ]
        
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_query, records)
                count = len(records)
        
        logger.info(f"Inserted/updated {count} KPI records")
        return count
    
    def get_stock_prices(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Query stock prices from the warehouse.
        
        Args:
            symbol: Optional symbol filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Maximum rows to return
            
        Returns:
            DataFrame with stock prices
        """
        query = """
            SELECT symbol, date, open_price, high_price, low_price, 
                   close_price, adjusted_close, volume
            FROM stock_prices
            WHERE 1=1
        """
        params = []
        
        if symbol:
            query += " AND symbol = %s"
            params.append(symbol)
        
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        
        query += f" ORDER BY date DESC LIMIT {limit}"
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=params or None)
        
        return df
    
    def get_stock_kpis(
        self,
        symbol: Optional[str] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        limit: int = 1000
    ) -> pd.DataFrame:
        """
        Query stock KPIs from the warehouse.
        
        Args:
            symbol: Optional symbol filter
            start_date: Optional start date filter
            end_date: Optional end date filter
            limit: Maximum rows to return
            
        Returns:
            DataFrame with KPIs
        """
        query = """
            SELECT symbol, date, daily_return, sma_7, sma_30,
                   volatility_7d, volatility_30d, avg_volume_7d,
                   price_range, price_range_pct
            FROM stock_kpis
            WHERE 1=1
        """
        params = []
        
        if symbol:
            query += " AND symbol = %s"
            params.append(symbol)
        
        if start_date:
            query += " AND date >= %s"
            params.append(start_date)
        
        if end_date:
            query += " AND date <= %s"
            params.append(end_date)
        
        query += f" ORDER BY date DESC LIMIT {limit}"
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn, params=params or None)
        
        return df
    
    def get_available_symbols(self) -> List[str]:
        """Get list of available stock symbols."""
        query = "SELECT DISTINCT symbol FROM stock_prices ORDER BY symbol"
        
        with self.get_connection() as conn:
            df = pd.read_sql(query, conn)
        
        return df['symbol'].tolist()
    
    def get_latest_kpis(self, symbols: Optional[List[str]] = None) -> pd.DataFrame:
        """
        Get the latest KPIs for each symbol.
        
        Args:
            symbols: Optional list of symbols to filter
            
        Returns:
            DataFrame with latest KPIs per symbol
        """
        query = """
            SELECT DISTINCT ON (symbol)
                symbol, date, daily_return, sma_7, sma_30,
                volatility_7d, volatility_30d, avg_volume_7d,
                price_range, price_range_pct
            FROM stock_kpis
        """
        
        if symbols:
            placeholders = ','.join(['%s'] * len(symbols))
            query += f" WHERE symbol IN ({placeholders})"
            query += " ORDER BY symbol, date DESC"
            
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn, params=symbols)
        else:
            query += " ORDER BY symbol, date DESC"
            with self.get_connection() as conn:
                df = pd.read_sql(query, conn)
        
        return df


def get_warehouse() -> DataWarehouse:
    """Get a DataWarehouse instance."""
    return DataWarehouse()


def load_kpis_to_warehouse(kpi_df: pd.DataFrame) -> int:
    """
    Main entry point for loading KPIs to the warehouse.
    
    This function is called by the Airflow DAG to load
    Gold layer data into PostgreSQL.
    
    Args:
        kpi_df: DataFrame with KPI data
        
    Returns:
        Number of rows inserted
    """
    warehouse = get_warehouse()
    return warehouse.insert_stock_kpis(kpi_df)


if __name__ == "__main__":
    # Test the warehouse connection
    warehouse = DataWarehouse()
    
    print("Available symbols:", warehouse.get_available_symbols())
    
    df = warehouse.get_latest_kpis()
    print("\nLatest KPIs:")
    print(df)
