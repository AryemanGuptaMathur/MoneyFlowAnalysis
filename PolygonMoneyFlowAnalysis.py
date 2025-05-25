import os
import time
import certifi
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime, timedelta
from polygon import RESTClient
import pytz

# Dashboard imports
import dash
from dash import dcc, html
import plotly.express as px
import plotly.graph_objects as go
from dash.dependencies import Input, Output

# Set your Polygon.io API key
API_KEY = os.getenv("POLYGON_API_KEY", "i1hUO0aGXnmk2zRHF1qEXmUeyAbjhqVC")

# Initialize the Polygon.io REST client
client = RESTClient(API_KEY)

# Time Frames
today = datetime.now()
date_offsets = {
    "1d": (today - timedelta(days=1)).strftime('%Y-%m-%d'),
    "1w": (today - timedelta(weeks=1)).strftime('%Y-%m-%d'),
    "1m": (today - timedelta(weeks=4)).strftime('%Y-%m-%d')
}

# Initialize the nested dictionary for GICS sectors
gics_sectors = {
    "Information Technology": {},
    #"Health Care": {},
   # "Financials": {},
    "Consumer Discretionary": {},
   # "Communication Services": {},
   # "Industrials": {},
    "Consumer Staples": {},
    "Energy": {},
    "Utilities": {},
    #"Real Estate": {},
    "Materials": {}
}

def get_stock_price(ticker: str, date: str = None) -> float:
    try:
        if date:
            aggs = client.get_aggs(ticker, 1, "day", from_=date, to=date)
            if aggs and len(aggs) > 0:
                return aggs[0].close
        else:
            close = client.get_previous_close_agg(ticker=ticker, adjusted=True)
            if close:
                return close[0].close
    except:
        pass
    return None

def get_market_cap(ticker: str) -> float:
    try:
        details = client.get_ticker_details(ticker)
        if details and hasattr(details, "market_cap"):
            return details.market_cap
    except:
        pass
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

def collect_sector_data():
    sp500_df = fetch_snp500_data()
    if sp500_df is None:
        return {}

    total = len(sp500_df)
    for idx, row in sp500_df.iterrows():
        ticker = row['Symbol'].replace('.', '-')
        sector = row['GICS Sector']

        if sector not in gics_sectors:
            continue

        current_price = get_stock_price(ticker)
        current_cap = get_market_cap(ticker)

        if not current_price or not current_cap:
            continue

        data = {
            "price_today": current_price,
            "market_cap_today": current_cap
        }

        for label, dt in date_offsets.items():
            hist_price = get_stock_price(ticker, dt)
            if hist_price:
                data[f"price_{label}"] = hist_price
                data[f"market_cap_{label}"] = (hist_price / current_price) * current_cap
            else:
                data[f"price_{label}"] = None
                data[f"market_cap_{label}"] = None

        gics_sectors[sector][ticker] = data
        print(f"Processed {ticker} ({idx + 1}/{total})")

    return gics_sectors

def compute_money_flows(gics_data):
    flow_result = {"1d": [], "1w": [], "1m": []}
    for sector, tickers in gics_data.items():
        sector_changes = {"sector": sector}
        for label in ["1d", "1w", "1m"]:
            pct_changes = []
            absolute_changes = []
            for d in tickers.values():
                try:
                    today = d["market_cap_today"]
                    past = d.get(f"market_cap_{label}")
                    if today and past:
                        pct = ((today - past) / past) * 100
                        abs_change = today - past
                        pct_changes.append(pct)
                        absolute_changes.append(abs_change)
                except:
                    continue
            avg_pct_change = sum(pct_changes) / len(pct_changes) if pct_changes else 0
            total_abs_change = sum(absolute_changes) if absolute_changes else 0
            # Convert to millions for better readability
            total_abs_change_millions = total_abs_change / 1_000_000
            
            sector_changes[label] = avg_pct_change
            sector_changes[f"{label}_abs"] = total_abs_change_millions
            
        for label in ["1d", "1w", "1m"]:
            flow_result[label].append({
                "Sector": sector, 
                "Change (%)": sector_changes[label],
                "Absolute Change ($M)": sector_changes[f"{label}_abs"]
            })
    return flow_result

# Collect Initial Data
print("Fetching data... this may take a few minutes.")
gics_data = collect_sector_data()
money_flows = compute_money_flows(gics_data)

# Get current EST timestamp
def get_est_timestamp():
    est = pytz.timezone('US/Eastern')
    return datetime.now(est).strftime("%Y-%m-%d %H:%M:%S %Z")

# Building the Dashboard App 
app = dash.Dash(__name__)
app.title = "Sector Money Flow Dashboard"

app.layout = html.Div([
    html.H2("S&P 500 Sector Money Flow", style={'textAlign': 'center'}),
    
    html.Div(id='timestamp-div', style={'textAlign': 'center', 'fontSize': '14px', 'marginBottom': '20px'}),
    
    html.Div([
        html.Div([
            html.H3("1 Day Change", style={'textAlign': 'center'}),
            dcc.Graph(id='1d-graph'),
        ], style={'width': '100%', 'marginBottom': '30px'}),
        
        html.Div([
            html.H3("1 Week Change", style={'textAlign': 'center'}),
            dcc.Graph(id='1w-graph'),
        ], style={'width': '100%', 'marginBottom': '30px'}),
        
        html.Div([
            html.H3("1 Month Change", style={'textAlign': 'center'}),
            dcc.Graph(id='1m-graph'),
        ], style={'width': '100%'}),
    ], style={'margin': '20px'}),

    dcc.Interval(
        id='interval-component',
        interval=30 * 60 * 1000,  # 30 minutes in milliseconds
        n_intervals=0
    )
])

@app.callback(
    [Output('1d-graph', 'figure'),
     Output('1w-graph', 'figure'),
     Output('1m-graph', 'figure'),
     Output('timestamp-div', 'children')],
    [Input('interval-component', 'n_intervals')]
)
def update_charts(n_intervals):
    global gics_data, money_flows

    # Refresh data on every interval
    if n_intervals > 0:
        print(f"[INFO] Refreshing data... (interval {n_intervals})")
        gics_data = collect_sector_data()
        money_flows = compute_money_flows(gics_data)

    # Get current EST timestamp
    timestamp = get_est_timestamp()
    
    figures = {}
    for timeframe in ['1d', '1w', '1m']:
        df = pd.DataFrame(money_flows[timeframe])
        
        # Sort by change percentage (ascending for horizontal bars to have highest at top)
        df = df.sort_values('Change (%)', ascending=True)
        
        # Create horizontal bar chart
        fig = go.Figure()
        
        # Add the horizontal bars
        fig.add_trace(go.Bar(
            x=df['Change (%)'],  # x-axis is now the percentage values
            y=df['Sector'],      # y-axis is now the sector names
            text=[f"{pct:.2f}%<br>${abs_val:.1f}M" for pct, abs_val in zip(df['Change (%)'], df['Absolute Change ($M)'])],
            textposition='auto',
            marker_color=[
                'green' if val > 0 else 'red' for val in df['Change (%)']
            ],
            name=f"{timeframe.upper()} Change",
            orientation='h'  # Make bars horizontal
        ))
        
        fig.update_layout(
            title=f"{timeframe.upper()} Change",
            xaxis_title="% Change",  # x-axis is now percentage
            yaxis_title="Sector",    # y-axis is now sectors
            margin=dict(l=150, r=50, t=50, b=50),  # More left margin for sector names
            yaxis=dict(
                tickfont=dict(size=10)
            ),
            showlegend=False,
            height=400
        )
        
        figures[timeframe] = fig
    
    timestamp_display = html.Div([
        html.P(f"Last Updated: {timestamp}"),
        html.P("Values shown: Percentage Change (%) and Absolute Change ($M)")
    ])
    
    return figures['1d'], figures['1w'], figures['1m'], timestamp_display

# Running the Server
if __name__ == '__main__':
    app.run(debug=False)
