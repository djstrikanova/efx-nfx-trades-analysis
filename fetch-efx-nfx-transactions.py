import requests
import time
import sqlite3
import json
from typing import Dict, List, Optional
from datetime import datetime
import logging
from requests.exceptions import RequestException
from pathlib import Path

class EOSHistoryFetcher:
    def __init__(
        self,
        target_account: str,
        db_path: str = "eos_history.db",
        max_retries: int = 3,
        delay_between_requests: int = 1
    ):
        """Initialize the EOS history fetcher"""
        self.target_account = target_account
        self.api_endpoint = "https://eos.greymass.com/v1/history/get_actions"
        self.max_retries = max_retries
        self.delay = delay_between_requests
        self.db_path = db_path
        
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )
        self.logger = logging.getLogger(__name__)
        
        self.init_db()

    def init_db(self):
        """Initialize SQLite database with flattened schema"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            # Create actions table with flattened structure
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS actions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    global_action_seq INTEGER UNIQUE,
                    block_num INTEGER,
                    block_time TEXT,
                    trx_id TEXT,
                    actor TEXT,
                    action_name TEXT,
                    from_account TEXT,
                    to_account TEXT,
                    memo TEXT,
                    quantity TEXT,
                    contract TEXT,
                    raw_data TEXT,
                    processed_at TEXT
                )
            ''')
            
            # Create fetch state table
            cursor.execute('''
                CREATE TABLE IF NOT EXISTS fetch_state (
                    account TEXT PRIMARY KEY,
                    last_position INTEGER,
                    updated_at TEXT
                )
            ''')
            
            # Create indexes for common queries
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_trx_id ON actions(trx_id)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_memo ON actions(memo)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_block_time ON actions(block_time)')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_from_to ON actions(from_account, to_account)')
            
            conn.commit()

    def get_stored_position(self) -> int:
        """Get the last processed position from database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                "SELECT last_position FROM fetch_state WHERE account = ?",
                (self.target_account,)
            )
            result = cursor.fetchone()
            return result[0] if result else 0

    def update_position(self, position: int):
        """Update the last processed position in database"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute('''
                INSERT OR REPLACE INTO fetch_state (account, last_position, updated_at)
                VALUES (?, ?, ?)
            ''', (self.target_account, position, datetime.utcnow().isoformat()))
            conn.commit()

    def fetch_actions(self, pos: int, offset: int = 100) -> Optional[Dict]:
        """Fetch actions from the EOS API with retry logic"""
        for attempt in range(self.max_retries):
            try:
                params = {
                    "account_name": self.target_account,
                    "pos": pos,
                    "offset": offset
                }
                
                response = requests.post(self.api_endpoint, json=params)
                response.raise_for_status()
                return response.json()
                
            except RequestException as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt < self.max_retries - 1:
                    wait_time = (attempt + 1) * 2
                    self.logger.info(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                else:
                    self.logger.error(f"Max retries reached for position {pos}")
                    raise

    def store_actions(self, actions: List[Dict]):
        """Store actions in SQLite database with flattened structure"""
        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()
            
            for action in actions:
                try:
                    act_trace = action['action_trace']
                    act = act_trace['act']
                    data = act.get('data', {})
                    
                    # Extract flattened data
                    action_data = {
                        'global_action_seq': action['global_action_seq'],
                        'block_num': action['block_num'],
                        'block_time': action['block_time'],
                        'trx_id': act_trace['trx_id'],
                        'actor': act['authorization'][0]['actor'] if act['authorization'] else None,
                        'action_name': act['name'],
                        'from_account': data.get('from'),
                        'to_account': data.get('to'),
                        'memo': data.get('memo'),
                        'quantity': data.get('quantity'),
                        'contract': act['account'],
                        'raw_data': json.dumps(action),
                        'processed_at': datetime.utcnow().isoformat()
                    }
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO actions (
                            global_action_seq, block_num, block_time, trx_id,
                            actor, action_name, from_account, to_account,
                            memo, quantity, contract, raw_data, processed_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        action_data['global_action_seq'],
                        action_data['block_num'],
                        action_data['block_time'],
                        action_data['trx_id'],
                        action_data['actor'],
                        action_data['action_name'],
                        action_data['from_account'],
                        action_data['to_account'],
                        action_data['memo'],
                        action_data['quantity'],
                        action_data['contract'],
                        action_data['raw_data'],
                        action_data['processed_at']
                    ))
                    
                except Exception as e:
                    self.logger.error(f"Error storing action {action.get('global_action_seq')}: {str(e)}")
            
            conn.commit()

    def fetch_all_history(self):
        """Main method to fetch all history"""
        pos = self.get_stored_position()
        self.logger.info(f"Starting from position {pos}")
        
        try:
            while True:
                self.logger.info(f"Fetching actions from position {pos}")
                result = self.fetch_actions(pos)
                
                if not result or not result.get('actions'):
                    self.logger.info("No more actions to fetch")
                    break
                
                actions = result['actions']
                self.store_actions(actions)
                
                pos += len(actions)
                self.update_position(pos)
                
                time.sleep(self.delay)
                
                self.logger.info(f"Processed {len(actions)} actions. New position: {pos}")
                
        except KeyboardInterrupt:
            self.logger.info("\nProcess interrupted by user")
            self.logger.info("Progress saved. Will resume from last position on next run.")
        except Exception as e:
            self.logger.error(f"Error during processing: {str(e)}")
            self.logger.info("Progress saved. Will resume from last position on next run.")

    def query_transactions(self, memos: List[str] = None, start_time: str = None, end_time: str = None) -> List[Dict]:
        """Query transactions with optional filters"""
        with sqlite3.connect(self.db_path) as conn:
            conn.row_factory = sqlite3.Row  # This enables accessing columns by name
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    block_time as time,
                    trx_id,
                    from_account as account,
                    memo,
                    quantity
                FROM actions 
                WHERE action_name = 'transfer'
            """
            params = []
            
            if memos:
                memo_placeholders = ','.join('?' * len(memos))
                query += f" AND memo IN ({memo_placeholders})"
                params.extend(memos)
            
            if start_time:
                query += " AND block_time >= ?"
                params.append(start_time)
            
            if end_time:
                query += " AND block_time <= ?"
                params.append(end_time)
            
            query += " ORDER BY block_time ASC"
            
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

def main():
    fetcher = EOSHistoryFetcher("effecttokens")
    
    try:
        # Fetch history
        fetcher.fetch_all_history()
        
        # Example: Query specific transactions
        memos = ["Defibox: swap token", "swap,8013829,437"]
        transactions = fetcher.query_transactions(memos=memos)
        
        print("\nExample Transactions:")
        for tx in transactions[:5]:  # Show first 5 transactions
            print(f"Time: {tx['time']}")
            print(f"TrxID: {tx['trx_id']}")
            print(f"Account: {tx['account']}")
            print(f"Memo: {tx['memo']}")
            print(f"Quantity: {tx['quantity']}")
            print("-" * 50)
        
    except KeyboardInterrupt:
        print("\nProcess interrupted by user")
        print("Progress saved. Will resume from last position on next run.")

if __name__ == "__main__":
    main()