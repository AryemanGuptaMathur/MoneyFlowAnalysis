import pandas as pd
import certifi
from bs4 import BeautifulSoup
import os
from datetime import datetime
from polygon import RESTClient
import requests

# Set your Polygon.io API key (replace with your actual key or use environment variable)
API_KEY = os.getenv("POLYGON_API_KEY", "i1hUO0aGXnmk2zRHF1qEXmUeyAbjhqVC")

# Initialize the Polygon.io REST client
client = RESTClient(API_KEY)

# Initialize the nested dictionary for GICS sectors
gics_sectors = {
    "Information Technology": {},
    "Health Care": {},
    "Financials": {},
    "Consumer Discretionary": {},
    "Communication Services": {},
    "Industrials": {},
    "Consumer Staples": {},
    "Energy": {},
    "Utilities": {},
    "Real Estate": {},
    "Materials": {}
}

def get_stock_price(ticker: str) -> float:
    """Fetch the latest closing price for the given ticker using RESTClient."""
    try:
        response = client.get_previous_close_agg(ticker=ticker, adjusted=True)
        if response:
            return response[0].close
        return None
    except Exception:
        return None

def get_market_cap(ticker: str) -> float:
    """Fetch the market capitalization for the given ticker using RESTClient."""
    try:
        details = client.get_ticker_details(ticker=ticker)
        if details and hasattr(details, "market_cap"):
            return details.market_cap
        return None
    except Exception:
        return None

def fetch_snp500_data():
    """Fetch S&P 500 constituents from Wikipedia."""
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    try:
        response = requests.get(url, verify=certifi.where())
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')
        table = soup.find('table', {'id': 'constituents'})
        sp500_df = pd.read_html(str(table))[0]
        return sp500_df
    except Exception as e:
        print(f"Error fetching S&P 500 data: {e}")
        return None

def main():
    print(f"Starting data collection on {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Fetch S&P 500 data
    sp500_df = fetch_snp500_data()
    if sp500_df is None:
        print("Failed to fetch S&P 500 constituents")
        return

    # Process each company
    total_tickers = len(sp500_df)
    for idx, row in sp500_df.iterrows():
        ticker = row['Symbol'].replace('.', '-')  # Convert BRK.B to BRK-B
        sector = row['GICS Sector']
        
        if sector in gics_sectors:
            # Fetch price and market cap
            price = get_stock_price(ticker)
            market_cap = get_market_cap(ticker)
            
            # Store data in dictionary
            gics_sectors[sector][ticker] = {
                "price": price,
                "market_cap": market_cap
            }
            
            # Print progress
            print(f"Processed {ticker} ({idx + 1}/{total_tickers})")

    # Summarize results
    for sector, tickers in gics_sectors.items():
        valid_tickers = sum(1 for t in tickers if tickers[t]["price"] is not None or tickers[t]["market_cap"] is not None)
        print(f"{sector}: {valid_tickers} tickers with data")
        # Show first 2 tickers as sample
        sample = dict(list(tickers.items())[:2])
        for ticker, data in sample.items():
            price = f"${data['price']:.2f}" if data['price'] is not None else "N/A"
            market_cap = f"${data['market_cap']:,.2f}" if data['market_cap'] is not None else "N/A"
            print(f"  {ticker}: Price={price}, Market Cap={market_cap}")

if __name__ == "__main__":
    main()