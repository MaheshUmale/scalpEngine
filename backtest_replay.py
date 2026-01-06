"""
Backtest Replay Engine

Transparently replays historical data via WebSocket for backtesting.
Java dashboard connects to same port and receives data in identical format.

Usage:
    python backtest_replay.py --date 2026-01-05 --speed 5
    
    --date:  Target date to replay (YYYY-MM-DD)
    --speed: Playback speed multiplier (1=realtime, 5=5x, 999=instant)
    --start: Start time (HH:MM, default: 09:15)
    --end:   End time (HH:MM, default: 15:30)
"""

import argparse
import asyncio
import websockets
import json
import sqlite3
from datetime import datetime, timedelta
import time

# Local modules
from backfill_trendlyne import DB as TrendlyneDB

PORT = 8765

class BacktestReplayEngine:
    def __init__(self, target_date, speed=1, start_time="09:15", end_time="15:30"):
        self.target_date = target_date
        self.speed = speed
        self.start_time = start_time
        self.end_time = end_time
        
        self.db_path = "backtest_data.db"
        self.clients = set()
        self.current_time = None
        self.is_playing = False
        
        # Load all candle data into memory for fast access
        self.candle_data = self._load_candle_data()
        
        print(f"[Replay] Loaded {len(self.candle_data)} minute intervals")
        
    def _load_candle_data(self):
        """Load all candle data from SQLite into memory grouped by timestamp"""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.cursor()
        
        cursor.execute("""SELECT * FROM backtest_candles 
                          WHERE date=? 
                          ORDER BY timestamp ASC""", (self.target_date,))
        
        rows = cursor.fetchall()
        conn.close()
        
        # Group by timestamp
        data_by_time = {}
        for row in rows:
            symbol, date, ts, open_, high, low, close, volume, source = row
            
            if ts not in data_by_time:
                data_by_time[ts] = []
            
            data_by_time[ts].append({
                'symbol': symbol,
                'timestamp_str': ts,
                'open': open_,
                'high': high,
                'low': low,
                'close': close,
                'volume': volume,
                'source': source
            })
        
        return data_by_time
    
    def _get_option_chain(self, symbol, current_time_str):
        """Fetch option chain data for given symbol and time from Trendlyne DB"""
        try:
            # Get closest option chain snapshot before or at current_time
            chain = TrendlyneDB.get_latest_chain(symbol)  # Gets latest available
            return chain if chain else []
        except:
            return []
    
    def _get_pcr(self, symbol):
        """Get PCR from Trendlyne DB"""
        try:
            agg = TrendlyneDB.get_latest_aggregates(symbol)
            return agg['pcr'] if agg else 1.0
        except:
            return 1.0
    
    async def register_client(self, websocket):
        """Register new WebSocket client"""
        self.clients.add(websocket)
        print(f"[Client] Connected ({len(self.clients)} total)")
        
    async def unregister_client(self, websocket):
        """Unregister WebSocket client"""
        self.clients.discard(websocket)
        print(f"[Client] Disconnected ({len(self.clients)} remaining)")
    
    async def broadcast_candles(self, timestamp_str):
        """Broadcast candle update for given timestamp"""
        if timestamp_str not in self.candle_data:
            return
        
        candles_at_time = self.candle_data[timestamp_str]
        
        # Convert to bridge format
        candle_updates = []
        for c in candles_at_time:
            # Calculate timestamp in epoch milliseconds
            dt = datetime.strptime(f"{self.target_date} {timestamp_str}", "%Y-%m-%d %H:%M")
            ts_ms = int(dt.timestamp() * 1000)
            
            candle_updates.append({
                "symbol": c['symbol'],
                "timestamp": ts_ms,
                "1m": {
                    "open": c['open'],
                    "high": c['high'],
                    "low": c['low'],
                    "close": c['close'],
                    "volume": c['volume'],
                    "vwap": c['close']  # Approximation
                },
                "5m": {  # Placeholder - would need aggregation logic
                    "open": c['open'],
                    "high": c['high'],
                    "low": c['low'],
                    "close": c['close'],
                    "volume": c['volume']
                },
                "pcr": self._get_pcr(c['symbol']) if c['symbol'] in ['NIFTY', 'BANKNIFTY'] else 1.0
            })
        
        message = {
            "type": "candle_update",
            "data": candle_updates
        }
        
        if self.clients:
            await asyncio.gather(
                *[client.send(json.dumps(message)) for client in self.clients],
                return_exceptions=True
            )

    async def broadcast_option_chain(self):
        """Broadcast option chain (once per minute)"""
        for symbol in ['NIFTY', 'BANKNIFTY']:
            chain = self._get_option_chain(symbol, self.current_time)
            if chain:
                message = {
                    "type": "option_chain",
                    "symbol": symbol,
                    "data": chain
                }
                
                if self.clients:
                    await asyncio.gather(
                        *[client.send(json.dumps(message)) for client in self.clients],
                        return_exceptions=True
                    )
    
    async def broadcast_pcr(self):
        """Broadcast PCR update"""
        for symbol in ['NIFTY', 'BANKNIFTY']:
            pcr = self._get_pcr(symbol)
            message = {
                "type": "pcr_update",
                "symbol": symbol,
                "pcr": pcr
            }
            
            if self.clients:
                await asyncio.gather(
                    *[client.send(json.dumps(message)) for client in self.clients],
                    return_exceptions=True
                )
    
    async def replay_loop(self):
        """Main replay loop"""
        print(f"\n[Replay] Starting playback: {self.start_time} -> {self.end_time} ({self.speed}x speed)")
        
        # Generate time sequence
        start_dt = datetime.strptime(f"{self.target_date} {self.start_time}", "%Y-%m-%d %H:%M")
        end_dt = datetime.strptime(f"{self.target_date} {self.end_time}", "%Y-%m-%d %H:%M")
        
        current_dt = start_dt
        
        while current_dt <= end_dt:
            self.current_time = current_dt.strftime("%H:%M")
            
            # Broadcast data for this minute
            await self.broadcast_candles(self.current_time)
            
            # Broadcast options every minute
            await self.broadcast_option_chain()
            await self.broadcast_pcr()
            
            print(f"[Replay] {self.current_time} | Clients: {len(self.clients)}")
            
            # Advance time
            current_dt += timedelta(minutes=1)
            
            # Sleep based on speed (1 minute / speed)
            if self.speed < 999:
                await asyncio.sleep(60 / self.speed)
        
        print(f"\n[Replay] Completed - Press Ctrl+C to exit")
    
    async def handle_client(self, websocket, path=None):
        """Handle individual WebSocket client connection (supports both old and new websockets API)"""
        await self.register_client(websocket)
        try:
            async for message in websocket:
                # Echo back or handle commands if needed
                pass
        except websockets.exceptions.ConnectionClosed:
            pass
        finally:
            await self.unregister_client(websocket)
    
    async def start_server(self):
        """Start WebSocket server"""
        async with websockets.serve(self.handle_client, "localhost", PORT):
            print(f"[Server] Backtest Replay running on ws://localhost:{PORT}")
            print(f"[Server] Date: {self.target_date} | Speed: {self.speed}x")
            # Wait for at least one client to connect
            print(f"[Server] Waiting for Java dashboard connection...")
            while not self.clients:
                await asyncio.sleep(1)
            
            print(f"[Server] Client connected. Starting replay in 2s...")
            await asyncio.sleep(2)
            await self.replay_loop()
            
            # Keep server alive
            await asyncio.Future()
    
    def run(self):
        """Execute replay server"""
        asyncio.run(self.start_server())

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Backtest Replay Engine")
    parser.add_argument('--date', type=str, default='2026-01-05',
                        help='Target date (YYYY-MM-DD)')
    parser.add_argument('--speed', type=int, default=1,
                        help='Playback speed (1=realtime, 5=5x, 999=instant)')
    parser.add_argument('--start', type=str, default='09:15',
                        help='Start time (HH:MM)')
    parser.add_argument('--end', type=str, default='15:30',
                        help='End time (HH:MM)')
    
    args = parser.parse_args()
    
    print("=" * 60)
    print("BACKTEST REPLAY ENGINE")
    print("=" * 60)
    
    engine = BacktestReplayEngine(
        target_date=args.date,
        speed=args.speed,
        start_time=args.start,
        end_time=args.end
    )
    
    engine.run()
