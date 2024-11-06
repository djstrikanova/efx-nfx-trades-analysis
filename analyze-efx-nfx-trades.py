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
            'fee_amount': fee_tx['amount']
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
        try:
            max_price = np.ceil(df['ratio'].max())
            bins = np.arange(0, max_price + 1, 1)
            labels = [f"{i:.0f}-{i+1:.0f}" for i in bins[:-1]]
            
            df['price_range'] = pd.cut(df['ratio'], bins=bins, labels=labels, right=False)
            
            # Basic statistics
            basic_stats = df.groupby('price_range').agg({
                'trx_id': 'count',
                'trader': 'nunique',
                'efx_amount': 'sum',
                'nfx_amount': 'sum',
                'ratio': ['mean', 'min', 'max']
            })

            # Calculate weighted mean safely
            def safe_weighted_mean(group):
                efx_sum = group['efx_amount'].sum()
                if efx_sum == 0:
                    return np.nan
                return (group['ratio'] * group['efx_amount']).sum() / efx_sum

            weighted_means = df.groupby('price_range').apply(safe_weighted_mean)
            
            # Flatten and rename columns
            basic_stats.columns = [
                'num_trades',
                'unique_traders',
                'efx_volume',
                'nfx_volume',
                'simple_mean',
                'min_ratio',
                'max_ratio'
            ]
            
            # Add weighted mean and volume percentage
            total_efx_volume = df['efx_amount'].sum()
            result = basic_stats.assign(
                weighted_mean=weighted_means,
                volume_percentage=(basic_stats['efx_volume'] / total_efx_volume * 100).round(2)
            )
            
            return result.round(4)

        except Exception as e:
            self.logger.error(f"Error in analyze_price_ranges: {str(e)}")
            raise


    def analyze_top_traders(self, df: pd.DataFrame) -> pd.DataFrame:
            """Analyze traders with both simple and weighted averages"""
            try:
                # Calculate basic stats more explicitly
                trader_stats = df.groupby('trader').agg({
                    'trx_id': 'count',
                    'efx_amount': 'sum',
                    'nfx_amount': 'sum'
                })
                
                # Calculate ratios separately
                ratio_stats = df.groupby('trader')['ratio'].agg(['min', 'max', 'mean']).rename(
                    columns={'mean': 'simple_mean_ratio'}
                )

                # Calculate weighted mean with error handling
                def safe_weighted_mean(group):
                    efx_sum = group['efx_amount'].sum()
                    if efx_sum == 0:
                        return np.nan
                    return (group['ratio'] * group['efx_amount']).sum() / efx_sum

                weighted_means = df.groupby('trader').apply(safe_weighted_mean).rename('weighted_mean_ratio')

                # Calculate volume percentage
                total_efx = df['efx_amount'].sum()
                volume_percentage = (trader_stats['efx_amount'] / total_efx * 100).round(4)

                # Combine all stats
                result = pd.concat([
                    trader_stats,
                    ratio_stats,
                    weighted_means,
                    volume_percentage.rename('volume_percentage')
                ], axis=1)

                # Rename columns for clarity
                result.columns = [
                    'trade_count',
                    'efx_volume',
                    'nfx_volume',
                    'min_ratio',
                    'max_ratio',
                    'simple_mean_ratio',
                    'weighted_mean_ratio',
                    'volume_percentage'
                ]

                # Sort by volume and round
                return result.sort_values('efx_volume', ascending=False).round(4)

            except Exception as e:
                self.logger.error(f"Error in analyze_top_traders: {str(e)}")
                raise


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
            
            # Price range analysis with weighted means
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
            
            # Enhanced trader analysis
            top_traders = self.analyze_top_traders(df)
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
        
        print("\nTop 5 Traders by Volume:")
        for trader in top_traders.head().index:
            stats = top_traders.loc[trader]
            print(f"\nTrader: {trader}")
            print(f"Trade Count: {stats['trade_count']:,}")
            print(f"EFX Volume: {stats['efx_volume']:,.2f} ({stats['volume_percentage']:.2f}%)")
            print(f"Simple Mean Ratio: {stats['simple_mean_ratio']:.4f}")
            print(f"Volume-Weighted Mean: {stats['weighted_mean_ratio']:.4f}")
            print(f"Ratio Range: {stats['min_ratio']:.4f} - {stats['max_ratio']:.4f}")
        
        print(f"\nAnalysis exported to {output_file}")

def main():
    analyzer = TradeAnalyzer()
    analyzer.analyze_and_export()

if __name__ == "__main__":
    main()