"""
Stock Data Pipeline Dashboard

Interactive Streamlit dashboard for visualizing stock data and KPIs
from the Gold layer of the data lake.

Features:
- Stock price charts with candlestick visualization
- KPI cards for key metrics
- Moving average trend analysis
- Volatility analysis
- Volume trend visualization
- Multi-stock comparison
"""

import os
from datetime import datetime, timedelta
from io import BytesIO
from typing import List, Optional

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import psycopg2
from minio import Minio

# Page configuration
st.set_page_config(
    page_title="Stock Data Pipeline Dashboard",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for better styling
st.markdown("""
<style>
    .stMetric {
        background-color: #1e1e1e;
        padding: 15px;
        border-radius: 10px;
        border: 1px solid #333;
    }
    .main-header {
        font-size: 2.5rem;
        font-weight: 700;
        background: linear-gradient(90deg, #00d4ff, #7b2cbf);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 1rem;
    }
    .sub-header {
        color: #888;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
        padding: 20px;
        border-radius: 15px;
        border: 1px solid #333;
    }
</style>
""", unsafe_allow_html=True)


# Database connection functions
@st.cache_resource
def get_db_connection():
    """Create database connection."""
    return psycopg2.connect(
        host=os.getenv("WAREHOUSE_HOST", "postgres"),
        port=int(os.getenv("WAREHOUSE_PORT", "5432")),
        database=os.getenv("WAREHOUSE_DB", "stockdb"),
        user=os.getenv("WAREHOUSE_USER", "airflow"),
        password=os.getenv("WAREHOUSE_PASSWORD", "airflow")
    )


@st.cache_resource
def get_minio_client():
    """Create MinIO client."""
    return Minio(
        os.getenv("MINIO_ENDPOINT", "minio:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin"),
        secure=False
    )


@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_available_symbols() -> List[str]:
    """Get list of available stock symbols."""
    try:
        conn = get_db_connection()
        df = pd.read_sql("SELECT DISTINCT symbol FROM stock_kpis ORDER BY symbol", conn)
        return df['symbol'].tolist()
    except Exception as e:
        st.warning(f"Could not fetch symbols from database: {e}")
        # Fallback: try to get from MinIO Gold layer
        try:
            client = get_minio_client()
            objects = client.list_objects("gold", prefix="kpis/daily/", recursive=True)
            symbols = set()
            for obj in objects:
                parts = obj.object_name.split('/')
                if len(parts) >= 3:
                    symbols.add(parts[2])
            return sorted(list(symbols))
        except:
            return ["AAPL", "GOOG", "MSFT", "AMZN"]


@st.cache_data(ttl=60)  # Cache for 1 minute
def get_stock_data(symbol: str) -> pd.DataFrame:
    """Get stock data from the warehouse or Gold layer."""
    try:
        conn = get_db_connection()
        query = """
            SELECT k.symbol, k.date, k.daily_return, k.sma_7, k.sma_30,
                   k.volatility_7d, k.volatility_30d, k.avg_volume_7d,
                   k.price_range, k.price_range_pct,
                   p.open_price, p.high_price, p.low_price, p.close_price,
                   p.adjusted_close, p.volume
            FROM stock_kpis k
            LEFT JOIN stock_prices p ON k.symbol = p.symbol AND k.date = p.date
            WHERE k.symbol = %s
            ORDER BY k.date DESC
            LIMIT 365
        """
        df = pd.read_sql(query, conn, params=[symbol])
        df['date'] = pd.to_datetime(df['date'])
        return df.sort_values('date')
    except Exception as e:
        st.warning(f"Database error: {e}. Trying Gold layer...")
        return get_gold_data(symbol)


def get_gold_data(symbol: str) -> pd.DataFrame:
    """Get data directly from Gold layer in MinIO."""
    try:
        client = get_minio_client()
        objects = list(client.list_objects("gold", prefix=f"kpis/daily/{symbol}/", recursive=True))
        
        if not objects:
            return pd.DataFrame()
        
        dfs = []
        for obj in objects:
            response = client.get_object("gold", obj.object_name)
            df = pd.read_parquet(BytesIO(response.read()))
            dfs.append(df)
            response.close()
            response.release_conn()
        
        combined = pd.concat(dfs, ignore_index=True)
        combined['date'] = pd.to_datetime(combined['date'])
        return combined.sort_values('date').drop_duplicates(subset=['date'])
    except Exception as e:
        st.error(f"Error fetching Gold layer data: {e}")
        return pd.DataFrame()


def create_price_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Create an interactive price chart with candlesticks."""
    fig = make_subplots(
        rows=2, cols=1,
        shared_xaxes=True,
        vertical_spacing=0.05,
        row_heights=[0.7, 0.3],
        subplot_titles=("Price", "Volume")
    )
    
    # Check if we have OHLC data
    has_ohlc = all(col in df.columns for col in ['open_price', 'high_price', 'low_price', 'close_price'])
    
    if has_ohlc and not df['open_price'].isna().all():
        # Candlestick chart
        fig.add_trace(
            go.Candlestick(
                x=df['date'],
                open=df['open_price'],
                high=df['high_price'],
                low=df['low_price'],
                close=df['close_price'],
                name='Price',
                increasing_line_color='#00d4ff',
                decreasing_line_color='#ff4444'
            ),
            row=1, col=1
        )
    elif 'close' in df.columns:
        # Line chart fallback
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['close'],
                mode='lines',
                name='Close Price',
                line=dict(color='#00d4ff', width=2)
            ),
            row=1, col=1
        )
    
    # Add moving averages
    if 'sma_7' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['sma_7'],
                mode='lines',
                name='7-Day SMA',
                line=dict(color='#ff9500', width=1.5)
            ),
            row=1, col=1
        )
    
    if 'sma_30' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['sma_30'],
                mode='lines',
                name='30-Day SMA',
                line=dict(color='#7b2cbf', width=1.5)
            ),
            row=1, col=1
        )
    
    # Volume chart
    if 'volume' in df.columns and not df['volume'].isna().all():
        colors = ['#00d4ff' if row.get('close_price', row.get('close', 0)) >= row.get('open_price', 0) 
                  else '#ff4444' for _, row in df.iterrows()]
        fig.add_trace(
            go.Bar(
                x=df['date'],
                y=df['volume'],
                name='Volume',
                marker_color=colors,
                opacity=0.7
            ),
            row=2, col=1
        )
    
    fig.update_layout(
        title=f"{symbol} Stock Price & Volume",
        template="plotly_dark",
        height=600,
        showlegend=True,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        xaxis_rangeslider_visible=False
    )
    
    return fig


def create_volatility_chart(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Create volatility analysis chart."""
    fig = go.Figure()
    
    if 'volatility_7d' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['volatility_7d'],
                mode='lines',
                name='7-Day Volatility',
                line=dict(color='#00d4ff', width=2),
                fill='tozeroy',
                fillcolor='rgba(0, 212, 255, 0.1)'
            )
        )
    
    if 'volatility_30d' in df.columns:
        fig.add_trace(
            go.Scatter(
                x=df['date'],
                y=df['volatility_30d'],
                mode='lines',
                name='30-Day Volatility',
                line=dict(color='#7b2cbf', width=2)
            )
        )
    
    fig.update_layout(
        title=f"{symbol} Volatility Analysis",
        template="plotly_dark",
        height=350,
        yaxis_title="Volatility (%)",
        xaxis_title="Date"
    )
    
    return fig


def create_returns_distribution(df: pd.DataFrame, symbol: str) -> go.Figure:
    """Create daily returns distribution histogram."""
    if 'daily_return' not in df.columns:
        return go.Figure()
    
    returns = df['daily_return'].dropna()
    
    fig = go.Figure()
    
    fig.add_trace(
        go.Histogram(
            x=returns,
            nbinsx=50,
            name='Daily Returns',
            marker_color='#00d4ff',
            opacity=0.7
        )
    )
    
    # Add normal distribution curve
    mean = returns.mean()
    std = returns.std()
    x_norm = np.linspace(returns.min(), returns.max(), 100)
    y_norm = (1 / (std * np.sqrt(2 * np.pi))) * np.exp(-0.5 * ((x_norm - mean) / std) ** 2)
    y_norm = y_norm * len(returns) * (returns.max() - returns.min()) / 50
    
    fig.add_trace(
        go.Scatter(
            x=x_norm,
            y=y_norm,
            mode='lines',
            name='Normal Distribution',
            line=dict(color='#ff9500', width=2)
        )
    )
    
    fig.update_layout(
        title=f"{symbol} Daily Returns Distribution",
        template="plotly_dark",
        height=350,
        xaxis_title="Daily Return (%)",
        yaxis_title="Frequency"
    )
    
    return fig


def create_comparison_chart(symbols: List[str]) -> go.Figure:
    """Create a comparison chart for multiple stocks."""
    fig = go.Figure()
    
    colors = ['#00d4ff', '#7b2cbf', '#ff9500', '#00ff88', '#ff4444']
    
    for i, symbol in enumerate(symbols):
        df = get_stock_data(symbol)
        if df.empty:
            continue
        
        # Normalize to percentage change from first day
        price_col = 'close' if 'close' in df.columns else 'adjusted_close'
        if price_col in df.columns:
            first_price = df[price_col].iloc[0]
            normalized = ((df[price_col] - first_price) / first_price) * 100
            
            fig.add_trace(
                go.Scatter(
                    x=df['date'],
                    y=normalized,
                    mode='lines',
                    name=symbol,
                    line=dict(color=colors[i % len(colors)], width=2)
                )
            )
    
    fig.update_layout(
        title="Stock Performance Comparison (Normalized %)",
        template="plotly_dark",
        height=400,
        yaxis_title="Return (%)",
        xaxis_title="Date",
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )
    
    return fig


# Main app
def main():
    # Header
    st.markdown('<p class="main-header">📈 Stock Data Pipeline Dashboard</p>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Real-time stock analysis powered by Medallion Architecture</p>', unsafe_allow_html=True)
    
    # Sidebar
    with st.sidebar:
        st.image("https://img.icons8.com/fluency/96/stocks-growth.png", width=80)
        st.title("Navigation")
        
        # Get available symbols
        symbols = get_available_symbols()
        
        if not symbols:
            st.warning("No stock data available yet. Run the pipeline first!")
            st.stop()
        
        # Symbol selection
        selected_symbol = st.selectbox("Select Stock", symbols, index=0)
        
        # Date range
        st.subheader("Date Range")
        date_range = st.slider(
            "Days of Data",
            min_value=7,
            max_value=365,
            value=90
        )
        
        # Multi-stock comparison
        st.subheader("Compare Stocks")
        compare_symbols = st.multiselect(
            "Select stocks to compare",
            symbols,
            default=symbols[:min(3, len(symbols))]
        )
        
        st.markdown("---")
        st.markdown("### Pipeline Status")
        
        # Check data freshness
        df = get_stock_data(selected_symbol)
        if not df.empty:
            latest_date = df['date'].max()
            st.success(f"Latest data: {latest_date.strftime('%Y-%m-%d')}")
        else:
            st.warning("No data available")
    
    # Main content
    if df.empty:
        st.info("No data available for the selected stock. Please run the data pipeline first.")
        st.markdown("""
        ### Getting Started
        1. Start the infrastructure: `docker-compose up -d`
        2. Open Airflow: [http://localhost:8080](http://localhost:8080)
        3. Trigger the `stock_data_pipeline` DAG
        4. Refresh this dashboard
        """)
        return
    
    # Filter by date range
    cutoff_date = datetime.now() - timedelta(days=date_range)
    df = df[df['date'] >= cutoff_date]
    
    # KPI Cards
    st.subheader(f"{selected_symbol} Key Metrics")
    
    col1, col2, col3, col4, col5 = st.columns(5)
    
    latest = df.iloc[-1] if len(df) > 0 else {}
    prev = df.iloc[-2] if len(df) > 1 else {}
    
    with col1:
        price = latest.get('close_price')
        if pd.isna(price): price = latest.get('close')
        if pd.isna(price): price = 0
        
        prev_price = prev.get('close_price') 
        if pd.isna(prev_price): prev_price = prev.get('close')
        if pd.isna(prev_price): prev_price = price
            
        delta = ((price - prev_price) / prev_price * 100) if prev_price else 0
        st.metric("Current Price", f"${price:.2f}", f"{delta:+.2f}%")
    
    with col2:
        daily_return = latest.get('daily_return')
        daily_return = 0 if pd.isna(daily_return) else daily_return
        st.metric("Daily Return", f"{daily_return:+.2f}%")
    
    with col3:
        vol_7d = latest.get('volatility_7d')
        vol_7d = 0 if pd.isna(vol_7d) else vol_7d
        st.metric("7D Volatility", f"{vol_7d:.2f}%")
    
    with col4:
        sma_7 = latest.get('sma_7')
        sma_7 = 0 if pd.isna(sma_7) else sma_7
        st.metric("7-Day SMA", f"${sma_7:.2f}")
    
    with col5:
        sma_30 = latest.get('sma_30')
        sma_30 = 0 if pd.isna(sma_30) else sma_30
        st.metric("30-Day SMA", f"${sma_30:.2f}")
    
    st.markdown("---")
    
    # Price Chart
    st.plotly_chart(create_price_chart(df, selected_symbol), use_container_width=True)
    
    # Two columns for additional charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.plotly_chart(create_volatility_chart(df, selected_symbol), use_container_width=True)
    
    with col2:
        st.plotly_chart(create_returns_distribution(df, selected_symbol), use_container_width=True)
    
    # Comparison chart
    if len(compare_symbols) > 1:
        st.markdown("---")
        st.subheader("Stock Comparison")
        st.plotly_chart(create_comparison_chart(compare_symbols), use_container_width=True)
    
    # Data table
    st.markdown("---")
    st.subheader("Raw Data")
    
    with st.expander("View Data Table"):
        # Fill NaN values to prevent format errors
        display_df = df.tail(50).copy()
        numeric_cols = ['close_price', 'daily_return', 'sma_7', 'sma_30', 
                       'volatility_7d', 'volatility_30d', 'volume',
                       'open_price', 'high_price', 'low_price', 'adjusted_close']
        for col in numeric_cols:
            if col in display_df.columns:
                display_df[col] = display_df[col].fillna(0)
        
        # Build format dict only for columns that exist
        format_dict = {}
        if 'close_price' in display_df.columns:
            format_dict['close_price'] = '${:.2f}'
        if 'daily_return' in display_df.columns:
            format_dict['daily_return'] = '{:+.2f}%'
        if 'sma_7' in display_df.columns:
            format_dict['sma_7'] = '${:.2f}'
        if 'sma_30' in display_df.columns:
            format_dict['sma_30'] = '${:.2f}'
        if 'volatility_7d' in display_df.columns:
            format_dict['volatility_7d'] = '{:.2f}%'
        if 'volatility_30d' in display_df.columns:
            format_dict['volatility_30d'] = '{:.2f}%'
        if 'volume' in display_df.columns:
            format_dict['volume'] = '{:,.0f}'
        
        st.dataframe(
            display_df.style.format(format_dict),
            use_container_width=True
        )
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style="text-align: center; color: #666;">
            <p>Built with ❤️ using Streamlit, MinIO, Airflow, and PostgreSQL</p>
            <p>Data Source: Alpha Vantage API | Medallion Architecture</p>
        </div>
        """,
        unsafe_allow_html=True
    )


if __name__ == "__main__":
    main()
