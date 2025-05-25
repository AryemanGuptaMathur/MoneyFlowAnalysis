import os
import time
import certifi
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from polygon import RESTClient
import pytz

# Set your Polygon.io API key
API_KEY = os.getenv("POLYGON_API_KEY", "i1hUO0aGXnmk2zRHF1qEXmUeyAbjhqVC")

# Initialize the Polygon.io REST client
client = RESTClient(API_KEY)

def fetch_snp500_tickers():
    """Fetch S&P 500 constituents from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        response = requests.get(url, verify=certifi.where())
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        sp500_df = pd.read_html(str(table))[0]
        return sp500_df['Symbol'].tolist()
    except Exception as e:
        print(f"Error fetching S&P 500 data: {e}")
        return []

def get_current_price_and_volume(ticker):
    """Get current price and volume for a ticker."""
    try:
        # Get current quote
        quote = client.get_last_quote(ticker)
        current_price = (quote.ask + quote.bid) / 2 if quote.ask and quote.bid else None
        
        # Get today's aggregate data for volume and VWAP
        today = datetime.now().strftime('%Y-%m-%d')
        aggs = client.get_aggs(ticker, 1, "day", from_=today, to=today)
        
        if aggs and len(aggs) > 0:
            agg = aggs[0]
            return {
                'volume': agg.volume,
                'vwap': agg.vwap,
                'open_price': agg.open,
                'current_price': current_price or agg.close,
                'high': agg.high,
                'low': agg.low
            }
    except Exception as e:
        print(f"Error getting current data for {ticker}: {e}")
    
    return None

def get_previous_close(ticker):
    """Get previous trading day's closing price."""
    try:
        prev_close = client.get_previous_close_agg(ticker=ticker, adjusted=True)
        if prev_close and len(prev_close) > 0:
            return prev_close[0].close
    except Exception as e:
        print(f"Error getting previous close for {ticker}: {e}")
    
    return None

def calculate_metrics(data, prev_close):
    """Calculate all the derived metrics from the handwritten notes."""
    if not data or not prev_close:
        return {}
    
    current_price = data['current_price']
    open_price = data['open_price']
    
    # Price changes
    price_change = current_price - prev_close
    price_change_pct = (price_change / prev_close) * 100 if prev_close else 0
    
    # Opening changes
    open_change = open_price - prev_close
    open_change_pct = (open_change / prev_close) * 100 if prev_close else 0
    
    # Intraday change (current vs open)
    intraday_change = current_price - open_price
    intraday_change_pct = (intraday_change / open_price) * 100 if open_price else 0
    
    # Price direction indicators
    price_up = current_price > prev_close
    price_up_since_open = current_price > open_price
    
    return {
        'price_change': price_change,
        'price_change_pct': price_change_pct,
        'open_change': open_change,
        'open_change_pct': open_change_pct,
        'intraday_change': intraday_change,
        'intraday_change_pct': intraday_change_pct,
        'price_up': price_up,
        'price_up_since_open': price_up_since_open
    }

def collect_intraday_data(tickers_subset=None, max_tickers=50):
    """Collect intraday data for S&P 500 stocks."""
    print("Fetching S&P 500 tickers...")
    all_tickers = fetch_snp500_tickers()
    
    if tickers_subset:
        tickers = tickers_subset
    else:
        # Limit to first N tickers to avoid API rate limits
        tickers = all_tickers[:max_tickers]
    
    print(f"Processing {len(tickers)} tickers...")
    
    data_list = []
    
    for i, ticker in enumerate(tickers):
        try:
            # Clean ticker symbol
            clean_ticker = ticker.replace('.', '-')
            
            print(f"Processing {clean_ticker} ({i+1}/{len(tickers)})")
            
            # Get current data
            current_data = get_current_price_and_volume(clean_ticker)
            if not current_data:
                print(f"  Skipping {clean_ticker} - no current data")
                continue
            
            # Get previous close
            prev_close = get_previous_close(clean_ticker)
            if not prev_close:
                print(f"  Skipping {clean_ticker} - no previous close")
                continue
            
            # Calculate metrics
            metrics = calculate_metrics(current_data, prev_close)
            
            # Combine all data
            row_data = {
                'ticker': clean_ticker,
                'volume': current_data['volume'],
                'vwap': current_data['vwap'],
                'open_price': current_data['open_price'],
                'current_price': current_data['current_price'],
                'prev_close': prev_close,
                'high': current_data['high'],
                'low': current_data['low'],
                **metrics
            }
            
            data_list.append(row_data)
            
            # Small delay to respect API rate limits
            time.sleep(0.1)
            
        except Exception as e:
            print(f"Error processing {ticker}: {e}")
            continue
    
    return pd.DataFrame(data_list)

def create_final_table(df):
    """Create and format the final output table."""
    if df.empty:
        print("No data collected.")
        return df
    
    # Round numeric columns for better display
    numeric_columns = ['volume', 'vwap', 'open_price', 'current_price', 'prev_close', 
                      'high', 'low', 'price_change', 'price_change_pct', 
                      'open_change', 'open_change_pct', 'intraday_change', 'intraday_change_pct']
    
    for col in numeric_columns:
        if col in df.columns:
            if col == 'volume':
                df[col] = df[col].round(0).astype(int)
            elif 'pct' in col:
                df[col] = df[col].round(2)
            else:
                df[col] = df[col].round(4)
    
    # Sort by price change percentage (descending)
    df = df.sort_values('price_change_pct', ascending=False)
    
    return df

def main():
    """Main execution function."""
    print("Starting intraday data collection...")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # You can specify specific tickers here, or leave None to use S&P 500
    # specific_tickers = ['AAPL', 'MSFT', 'GOOGL', 'AMZN', 'TSLA']
    specific_tickers = None
    
    # Collect data
    df = collect_intraday_data(tickers_subset=specific_tickers, max_tickers=50)
    
    if df.empty:
        print("No data was collected. Please check your API key and connection.")
        return
    
    # Create final formatted table
    final_df = create_final_table(df)
    
    # Display results
    print("\n" + "="*100)
    print("INTRADAY DATA SUMMARY")
    print("="*100)
    print(f"Total stocks processed: {len(final_df)}")
    print(f"Data collection completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Display top performers
    print("\nTOP 10 PERFORMERS (by % change):")
    print("-" * 100)
    top_10 = final_df.head(10)
    display_columns = ['ticker', 'current_price', 'prev_close', 'price_change', 
                      'price_change_pct', 'volume', 'vwap']
    print(top_10[display_columns].to_string(index=False))
    
    # Display bottom performers
    print("\nBOTTOM 10 PERFORMERS (by % change):")
    print("-" * 100)
    bottom_10 = final_df.tail(10)
    print(bottom_10[display_columns].to_string(index=False))
    
    # Save to CSV
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = f"intraday_data_{timestamp}.csv"
    final_df.to_csv(filename, index=False)
    print(f"\nFull data saved to: {filename}")
    
    # Print column explanations
    print("\nCOLUMN EXPLANATIONS:")
    print("-" * 50)
    print("ticker: Stock symbol")
    print("volume: Trading volume for the day")
    print("vwap: Volume Weighted Average Price")
    print("open_price: Opening price today")
    print("current_price: Current/latest price")
    print("prev_close: Previous trading day's closing price")
    print("price_change: Current price - Previous close")
    print("price_change_pct: Percentage change from previous close")
    print("open_change: Open price - Previous close")
    print("open_change_pct: Percentage change from previous close to open")
    print("intraday_change: Current price - Open price")
    print("intraday_change_pct: Percentage change from open to current")
    print("price_up: Boolean - Is current price > previous close")
    print("price_up_since_open: Boolean - Is current price > open price")
    
    return final_df

if __name__ == "__main__":
    # Run the data collection
    result_df = main()