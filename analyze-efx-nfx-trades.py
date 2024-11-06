import sqlite3
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class TradeAnalyzer:
    def __init__(self, db_path: str = "eos_history.db"):
        self.db_path = db_path
        self.logger = logger

    def parse_quantity(self, quantity_str: str) -> Tuple[float, str]:
        """Parse quantity string into amount and token"""
        try:
            if not quantity_str:
                return 0.0, ''
            amount_str, token = quantity_str.strip().split(' ')
            return float(amount_str), token
        except Exception as e:
            self.logger.error(f"Error parsing quantity '{quantity_str}': {str(e)}")
            return 0.0, ''

    def get_trades(self) -> List[Dict]:
        """Find and analyze all EFX/NFX trades"""
        trades = []
        
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            
            cursor.execute("""
                SELECT 
                    trx_id,
                    block_time as time,
                    actor,
                    from_account,
                    to_account,
                    memo,
                    quantity
                FROM actions
                WHERE 
                    action_name = 'transfer'
                    AND (
                        memo LIKE 'swap,%'
                        OR memo = 'Defibox: swap token'
                        OR to_account = 'fees.defi'
                    )
                ORDER BY block_time, trx_id
            """)
            
            rows = cursor.fetchall()

        # Group transactions by trx_id
        current_trx = None
        current_group = []
        
        for row in rows:
            if row['trx_id'] != current_trx:
                if current_group:
                    trade = self.analyze_trade_group(current_group)
                    if trade:
                        trades.append(trade)
                current_trx = row['trx_id']
                current_group = [dict(row)]
            else:
                current_group.append(dict(row))
        
        # Process last group
        if current_group:
            trade = self.analyze_trade_group(current_group)
            if trade:
                trades.append(trade)

        return trades

    def analyze_trade_group(self, transactions: List[Dict]) -> Dict:
        """Analyze a group of transactions with the same trx_id"""
        # Only process groups with exactly three actions
        if len(transactions) != 3:
            return None
            
        efx_tx = None
        nfx_tx = None
        fee_tx = None
        
        for tx in transactions:
            # Categorize each transaction
            amount, token = self.parse_quantity(tx['quantity'])
            
            if tx['to_account'] == 'fees.defi':
                fee_tx = {**tx, 'amount': amount}
            elif token == 'EFX':
                efx_tx = {**tx, 'amount': amount}
            elif token == 'NFX':
                nfx_tx = {**tx, 'amount': amount}
        
        # Verify we have all components of a valid trade
        if not all([efx_tx, nfx_tx, fee_tx]):
            return None
            
        # Calculate ratio as EFX/NFX regardless of direction
        ratio = efx_tx['amount'] / nfx_tx['amount']
        
        # Determine trade direction and trader
        if 'swap,' in efx_tx['memo']:
            direction = 'EFX_TO_NFX'
            trader = efx_tx['from_account']
        else:
            direction = 'NFX_TO_EFX'
            trader = nfx_tx['from_account']
        
        return {
            'timestamp': efx_tx['time'],
            'trx_id': efx_tx['trx_id'],
            'trader': trader,
            'direction': direction,
            'efx_amount': efx_tx['amount'],
            'nfx_amount': nfx_tx['amount'],
            'ratio': ratio,
            'fee_amount': fee_tx['amount']  # Including fee amount in case it's useful
        }

    def calculate_vwap(self, df: pd.DataFrame) -> float:
        """Calculate volume-weighted average price"""
        return (df['ratio'] * df['efx_amount']).sum() / df['efx_amount'].sum()

    def calculate_daily_average(self, df: pd.DataFrame) -> float:
        """Calculate average of daily averages"""
        daily_averages = df.groupby(df['timestamp'].dt.date)['ratio'].mean()
        return daily_averages.mean()

    def analyze_price_ranges(self, df: pd.DataFrame) -> pd.DataFrame:
        """Analyze trading activity by price ranges"""
        # Create price range bins
        max_price = np.ceil(df['ratio'].max())
        bins = np.arange(0, max_price + 1, 1)
        labels = [f"{i:.0f}-{i+1:.0f}" for i in bins[:-1]]
        
        # Add price range column
        df['price_range'] = pd.cut(df['ratio'], bins=bins, labels=labels, right=False)
        
        # Group by price range and calculate statistics
        price_analysis = df.groupby('price_range').agg({
            'trx_id': 'count',
            'trader': 'nunique',
            'efx_amount': 'sum',
            'nfx_amount': 'sum',
            'ratio': ['mean', 'min', 'max']
        }).round(4)
        
        price_analysis.columns = [
            'num_trades',
            'unique_traders',
            'efx_volume',
            'nfx_volume',
            'avg_ratio',
            'min_ratio',
            'max_ratio'
        ]
        
        # Calculate percentage of total volume
        total_efx_volume = df['efx_amount'].sum()
        price_analysis['volume_percentage'] = (
            (price_analysis['efx_volume'] / total_efx_volume * 100)
            .round(2)
        )
        
        return price_analysis

    def analyze_and_export(self, output_file: str = "efx_nfx_trades.xlsx"):
        """Analyze trades and export to Excel with enhanced statistics"""
        self.logger.info("Starting trade analysis...")
        
        trades = self.get_trades()
        
        if not trades:
            self.logger.warning("No trades found!")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(trades)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        # Get date range
        first_trade = df['timestamp'].min()
        last_trade = df['timestamp'].max()
        date_range = f"{first_trade} to {last_trade}"
        
        # Calculate all average types
        vwap = self.calculate_vwap(df)
        simple_avg = df['ratio'].mean()
        daily_avg = self.calculate_daily_average(df)
        
        # Calculate statistics
        stats = {
            'date_range': date_range,
            'total_trades': len(df),
            'unique_traders': df['trader'].nunique(),
            'total_efx_volume': df['efx_amount'].sum(),
            'total_nfx_volume': df['nfx_amount'].sum(),
            'vwap_ratio': vwap,
            'simple_avg_ratio': simple_avg,
            'daily_avg_ratio': daily_avg,
            'min_ratio': df['ratio'].min(),
            'max_ratio': df['ratio'].max(),
            'efx_to_nfx_trades': len(df[df['direction'] == 'EFX_TO_NFX']),
            'nfx_to_efx_trades': len(df[df['direction'] == 'NFX_TO_EFX'])
        }
        
        # Export to Excel
        with pd.ExcelWriter(output_file) as writer:
            # All trades
            df.to_excel(
                writer,
                sheet_name='All Trades',
                index=False,
                columns=['timestamp', 'trx_id', 'trader', 'direction', 
                        'efx_amount', 'nfx_amount', 'ratio', 'fee_amount']
            )
            
            # Price range analysis
            price_analysis = self.analyze_price_ranges(df)
            price_analysis.to_excel(writer, sheet_name='Price Analysis')
            
            # Daily breakdown with all metrics
            daily_stats = df.groupby('date').agg({
                'trx_id': 'count',
                'trader': 'nunique',
                'ratio': ['mean', 'min', 'max'],
                'efx_amount': 'sum',
                'nfx_amount': 'sum'
            }).round(4)
            
            # Add daily VWAP
            daily_stats['vwap'] = df.groupby('date').apply(self.calculate_vwap)
            daily_stats.to_excel(writer, sheet_name='Daily Stats')
            
            # Summary statistics
            pd.DataFrame([stats]).to_excel(writer, sheet_name='Summary', index=False)
            
            # Top traders analysis
            top_traders = df.groupby('trader').agg({
                'trx_id': 'count',
                'efx_amount': 'sum',
                'nfx_amount': 'sum',
                'ratio': ['mean', 'min', 'max']
            }).round(4)
            
            top_traders = top_traders.sort_values(('efx_amount', 'sum'), ascending=False)
            top_traders.to_excel(writer, sheet_name='Trader Analysis')
        
        # Print summary
        print("\nTrade Analysis Summary:")
        print(f"Date Range: {date_range}")
        print(f"Total trades analyzed: {stats['total_trades']:,}")
        print(f"Unique traders: {stats['unique_traders']:,}")
        print(f"EFX to NFX trades: {stats['efx_to_nfx_trades']:,}")
        print(f"NFX to EFX trades: {stats['nfx_to_efx_trades']:,}")
        print(f"\nTotal EFX volume: {stats['total_efx_volume']:,.4f}")
        print(f"Total NFX volume: {stats['total_nfx_volume']:,.4f}")
        print("\nPrice Averages (EFX/NFX):")
        print(f"Volume-Weighted Average: {stats['vwap_ratio']:.4f}")
        print(f"Simple Average: {stats['simple_avg_ratio']:.4f}")
        print(f"Daily Average: {stats['daily_avg_ratio']:.4f}")
        print(f"Ratio range: {stats['min_ratio']:.4f} - {stats['max_ratio']:.4f}")
        
        print("\nTrading Activity by Price Range:")
        print(price_analysis[['num_trades', 'efx_volume', 'volume_percentage']].to_string())
        
        print(f"\nAnalysis exported to {output_file}")
        """Analyze trades and export to Excel with enhanced statistics"""
        self.logger.info("Starting trade analysis...")
        
        # Get trade data
        trades = self.get_trades()
        
        if not trades:
            self.logger.warning("No trades found!")
            return
        
        # Convert to DataFrame
        df = pd.DataFrame(trades)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['date'] = df['timestamp'].dt.date
        
        # Get date range
        first_trade = df['timestamp'].min()
        last_trade = df['timestamp'].max()
        date_range = f"{first_trade} to {last_trade}"
        
        # Calculate VWAP
        vwap = self.calculate_vwap(df)
        
        # Analyze price ranges
        price_analysis = self.analyze_price_ranges(df)
        
        # Calculate statistics
        stats = {
            'date_range': date_range,
            'total_trades': len(df),
            'unique_traders': df['trader'].nunique(),
            'total_efx_volume': df['efx_amount'].sum(),
            'total_nfx_volume': df['nfx_amount'].sum(),
            'vwap_ratio': vwap,
            'simple_avg_ratio': df['ratio'].mean(),
            'min_ratio': df['ratio'].min(),
            'max_ratio': df['ratio'].max(),
            'efx_to_nfx_trades': len(df[df['direction'] == 'EFX_TO_NFX']),
            'nfx_to_efx_trades': len(df[df['direction'] == 'NFX_TO_EFX'])
        }
        
        # Export to Excel
        with pd.ExcelWriter(output_file) as writer:
            # All trades
            df.to_excel(
                writer,
                sheet_name='All Trades',
                index=False,
                columns=['timestamp', 'trx_id', 'trader', 'direction', 
                        'efx_amount', 'nfx_amount', 'ratio']
            )
            
            # Price range analysis
            price_analysis.to_excel(writer, sheet_name='Price Analysis')
            
            # Summary statistics
            pd.DataFrame([stats]).to_excel(writer, sheet_name='Summary', index=False)
            
            # Top traders by volume
            top_traders = df.groupby('trader').agg({
                'trx_id': 'count',
                'efx_amount': 'sum',
                'nfx_amount': 'sum',
                'ratio': ['mean', 'min', 'max']
            }).round(4)
            
            top_traders = top_traders.sort_values(('efx_amount', 'sum'), ascending=False)
            top_traders.to_excel(writer, sheet_name='Trader Analysis')
        
        # Print summary
        print("\nTrade Analysis Summary:")
        print(f"Date Range: {date_range}")
        print(f"Total trades analyzed: {stats['total_trades']:,}")
        print(f"Unique traders: {stats['unique_traders']:,}")
        print(f"EFX to NFX trades: {stats['efx_to_nfx_trades']:,}")
        print(f"NFX to EFX trades: {stats['nfx_to_efx_trades']:,}")
        print(f"\nTotal EFX volume: {stats['total_efx_volume']:,.4f}")
        print(f"Total NFX volume: {stats['total_nfx_volume']:,.4f}")
        print(f"\nVolume-Weighted Average EFX/NFX ratio: {stats['vwap_ratio']:.4f}")
        print(f"Simple Average EFX/NFX ratio: {stats['simple_avg_ratio']:.4f}")
        print(f"Ratio range: {stats['min_ratio']:.4f} - {stats['max_ratio']:.4f}")
        
        print("\nTrading Activity by Price Range:")
        print(price_analysis[['num_trades', 'efx_volume', 'volume_percentage']].to_string())
        
        print(f"\nAnalysis exported to {output_file}")

def main():
    analyzer = TradeAnalyzer()
    analyzer.analyze_and_export()

if __name__ == "__main__":
    main()