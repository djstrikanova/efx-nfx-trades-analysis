import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
from datetime import datetime
import numpy as np

class TradeVisualizer:
    def __init__(self, data_file: str = "efx_nfx_trades.xlsx"):
        self.data_file = data_file
        self.load_data()
        
    def load_data(self):
        """Load data from Excel file"""
        try:
            self.trades_df = pd.read_excel(self.data_file, sheet_name='All Trades')
            self.trades_df['timestamp'] = pd.to_datetime(self.trades_df['timestamp'])
            self.trades_df['date'] = self.trades_df['timestamp'].dt.date

            # Calculate price ranges for volume distribution
            bins = np.arange(0, np.ceil(self.trades_df['ratio'].max()) + 1, 1)
            self.trades_df['price_range'] = pd.cut(self.trades_df['ratio'], bins=bins)
            self.price_analysis = self.trades_df.groupby('price_range').agg({
                'efx_amount': 'sum',
                'trx_id': 'count'
            }).reset_index()

        except Exception as e:
            print(f"Error loading data: {str(e)}")
            raise

    def get_summary_stats(self):
        """Calculate summary statistics"""
        stats = {
            'date_range': f"{self.trades_df['timestamp'].min():%Y-%m-%d} to {self.trades_df['timestamp'].max():%Y-%m-%d}",
            'total_trades': len(self.trades_df),
            'unique_traders': self.trades_df['trader'].nunique(),
            'total_efx_volume': self.trades_df['efx_amount'].sum(),
            'total_nfx_volume': self.trades_df['nfx_amount'].sum(),
            'vwap_ratio': (self.trades_df['ratio'] * self.trades_df['efx_amount']).sum() / self.trades_df['efx_amount'].sum(),
            'simple_avg_ratio': self.trades_df['ratio'].mean(),
            'daily_avg_ratio': self.trades_df.groupby('date')['ratio'].mean().mean(),
            'min_ratio': self.trades_df['ratio'].min(),
            'max_ratio': self.trades_df['ratio'].max(),
            'efx_to_nfx_trades': len(self.trades_df[self.trades_df['direction'] == 'EFX_TO_NFX']),
            'nfx_to_efx_trades': len(self.trades_df[self.trades_df['direction'] == 'NFX_TO_EFX'])
        }
        return stats

    def create_combined_html(self, output_file: str = "efx_nfx_analysis.html"):
        """Create a single HTML file with all visualizations and summary stats"""
        # Get summary stats
        stats = self.get_summary_stats()
        
        # Create the HTML content
        html_content = f"""
        <html>
        <head>
            <title>EFX/NFX Trading Analysis</title>
            <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
            <style>
                body {{ font-family: Arial, sans-serif; margin: 20px; }}
                .summary-box {{
                    background-color: #f5f5f5;
                    padding: 20px;
                    border-radius: 10px;
                    margin-bottom: 20px;
                }}
                .stat-grid {{
                    display: grid;
                    grid-template-columns: repeat(3, 1fr);
                    gap: 20px;
                    margin-top: 20px;
                }}
                .stat-item {{
                    background-color: white;
                    padding: 15px;
                    border-radius: 5px;
                    box-shadow: 0 2px 4px rgba(0,0,0,0.1);
                }}
                .chart-container {{
                    margin-bottom: 40px;
                }}
                .value {{ 
                    font-size: 1.2em;
                    font-weight: bold;
                    color: #2c3e50;
                }}
                h1, h2 {{ color: #2c3e50; }}
            </style>
        </head>
        <body>
            <h1>EFX/NFX Trading Analysis</h1>
            
            <div class="summary-box">
                <h2>Summary Statistics</h2>
                <div class="stat-grid">
                    <div class="stat-item">
                        <div>Date Range</div>
                        <div class="value">{stats['date_range']}</div>
                    </div>
                    <div class="stat-item">
                        <div>Total Trades</div>
                        <div class="value">{stats['total_trades']:,}</div>
                    </div>
                    <div class="stat-item">
                        <div>Unique Traders</div>
                        <div class="value">{stats['unique_traders']:,}</div>
                    </div>
                    <div class="stat-item">
                        <div>Total EFX Volume</div>
                        <div class="value">{stats['total_efx_volume']:,.2f}</div>
                    </div>
                    <div class="stat-item">
                        <div>Total NFX Volume</div>
                        <div class="value">{stats['total_nfx_volume']:,.2f}</div>
                    </div>
                    <div class="stat-item">
                        <div>EFX/NFX Trade Direction</div>
                        <div class="value">EFX to NFX: {stats['efx_to_nfx_trades']:,}<br>NFX to EFX: {stats['nfx_to_efx_trades']:,}</div>
                    </div>
                    <div class="stat-item">
                        <div>Volume-Weighted Average</div>
                        <div class="value">{stats['vwap_ratio']:.4f}</div>
                    </div>
                    <div class="stat-item">
                        <div>Simple Average</div>
                        <div class="value">{stats['simple_avg_ratio']:.4f}</div>
                    </div>
                    <div class="stat-item">
                        <div>Daily Average</div>
                        <div class="value">{stats['daily_avg_ratio']:.4f}</div>
                    </div>
                </div>
            </div>
            
            <div class="chart-container" id="price-chart"></div>
            <div class="chart-container" id="volume-dist"></div>
            <div class="chart-container" id="trader-analysis"></div>
        </body>
        </html>
        """

        # Create and add charts
        # Price Chart
        daily_data = self.trades_df.groupby('date').agg({
            'ratio': ['mean', 'min', 'max'],
            'efx_amount': 'sum'
        }).reset_index()
        daily_data.columns = ['date', 'avg_ratio', 'min_ratio', 'max_ratio', 'volume']
        
        daily_data['MA7'] = daily_data['avg_ratio'].rolling(window=7).mean()
        daily_data['MA30'] = daily_data['avg_ratio'].rolling(window=30).mean()

        fig1 = make_subplots(rows=2, cols=1, shared_xaxes=True, 
                           vertical_spacing=0.03, row_heights=[0.7, 0.3])

        fig1.add_trace(go.Scatter(x=daily_data['date'], y=daily_data['avg_ratio'],
                                 name='Daily Average', line=dict(color='blue')), row=1, col=1)
        fig1.add_trace(go.Scatter(x=daily_data['date'], y=daily_data['MA7'],
                                 name='7-day MA', line=dict(color='orange', dash='dash')), row=1, col=1)
        fig1.add_trace(go.Scatter(x=daily_data['date'], y=daily_data['MA30'],
                                 name='30-day MA', line=dict(color='red', dash='dash')), row=1, col=1)
        fig1.add_trace(go.Bar(x=daily_data['date'], y=daily_data['volume'],
                             name='Volume (EFX)', marker_color='lightblue'), row=2, col=1)

        fig1.update_layout(height=600, title='EFX/NFX Daily Price and Volume',
                          yaxis_title='EFX/NFX Ratio', yaxis2_title='Volume (EFX)')

        # Volume Distribution
        fig2 = make_subplots(rows=1, cols=2, subplot_titles=('Volume by Price Range', 'Trade Count by Price Range'))
        
        fig2.add_trace(go.Bar(x=[str(x) for x in self.price_analysis['price_range']], 
                             y=self.price_analysis['efx_amount'],
                             name='Volume (EFX)', marker_color='lightblue'), row=1, col=1)
        fig2.add_trace(go.Bar(x=[str(x) for x in self.price_analysis['price_range']], 
                             y=self.price_analysis['trx_id'],
                             name='Number of Trades', marker_color='lightgreen'), row=1, col=2)
        
        fig2.update_layout(height=400, title='Trading Activity Distribution',
                          xaxis_title='Price Range (EFX/NFX)', xaxis2_title='Price Range (EFX/NFX)',
                          yaxis_title='Volume (EFX)', yaxis2_title='Number of Trades')

        # Trader Analysis
        top_traders = self.trades_df.groupby('trader').agg({
            'efx_amount': 'sum',
            'trx_id': 'count'
        }).nlargest(20, 'efx_amount')

        fig3 = make_subplots(rows=2, cols=1, subplot_titles=('Top 20 Traders by Volume', 'Trade Count'))
        
        fig3.add_trace(go.Bar(x=top_traders.index, y=top_traders['efx_amount'],
                             name='Volume (EFX)', marker_color='lightblue'), row=1, col=1)
        fig3.add_trace(go.Bar(x=top_traders.index, y=top_traders['trx_id'],
                             name='Number of Trades', marker_color='lightgreen'), row=2, col=1)
        
        fig3.update_layout(height=800, title='Top Trader Analysis',
                          showlegend=True)
        fig3.update_xaxes(tickangle=45)

        # Write the complete HTML file
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
            f.write(f"<script>")
            f.write(f"var price_chart = {fig1.to_json()};")
            f.write(f"var volume_dist = {fig2.to_json()};")
            f.write(f"var trader_analysis = {fig3.to_json()};")
            f.write("""
                Plotly.newPlot('price-chart', price_chart.data, price_chart.layout);
                Plotly.newPlot('volume-dist', volume_dist.data, volume_dist.layout);
                Plotly.newPlot('trader-analysis', trader_analysis.data, trader_analysis.layout);
            </script>""")

def main():
    visualizer = TradeVisualizer("efx_nfx_trades.xlsx")
    visualizer.create_combined_html()

if __name__ == "__main__":
    main()