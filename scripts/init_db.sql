-- Create the stock database for data warehouse
CREATE DATABASE stockdb;

-- Connect to stockdb and create tables
\c stockdb;

-- Stock daily prices table (Silver layer reference)
CREATE TABLE IF NOT EXISTS stock_prices (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    open_price DECIMAL(12, 4),
    high_price DECIMAL(12, 4),
    low_price DECIMAL(12, 4),
    close_price DECIMAL(12, 4),
    adjusted_close DECIMAL(12, 4),
    volume BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

-- Stock KPIs table (Gold layer)
CREATE TABLE IF NOT EXISTS stock_kpis (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    date DATE NOT NULL,
    daily_return DECIMAL(10, 6),
    sma_7 DECIMAL(12, 4),
    sma_30 DECIMAL(12, 4),
    volatility_7d DECIMAL(10, 6),
    volatility_30d DECIMAL(10, 6),
    avg_volume_7d DECIMAL(18, 2),
    price_range DECIMAL(12, 4),
    price_range_pct DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, date)
);

-- Summary statistics table (Gold layer aggregations)
CREATE TABLE IF NOT EXISTS stock_summary (
    id SERIAL PRIMARY KEY,
    symbol VARCHAR(10) NOT NULL,
    report_date DATE NOT NULL,
    period VARCHAR(20) NOT NULL,  -- 'daily', 'weekly', 'monthly'
    avg_close DECIMAL(12, 4),
    max_high DECIMAL(12, 4),
    min_low DECIMAL(12, 4),
    total_volume BIGINT,
    avg_daily_return DECIMAL(10, 6),
    max_daily_return DECIMAL(10, 6),
    min_daily_return DECIMAL(10, 6),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(symbol, report_date, period)
);

-- Create indexes for better query performance
CREATE INDEX idx_stock_prices_symbol_date ON stock_prices(symbol, date);
CREATE INDEX idx_stock_kpis_symbol_date ON stock_kpis(symbol, date);
CREATE INDEX idx_stock_summary_symbol ON stock_summary(symbol, report_date);
