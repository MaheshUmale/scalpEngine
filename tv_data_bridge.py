"""
TV Data Bridge - Multi-Source Redundancy WebSocket Server

Broadcasts real-time market data via WebSocket with three-tier fallback redundancy.

Data Feeds:
    - candle_update: 1m and 5m OHLC snapshots (60s interval)
    - market_breadth: NSE advance/decline/unchanged counts (60s interval)
    - option_chain: Strike-wise OI and Greeks from Trendlyne DB (60s interval)
    - pcr_update: Put-Call Ratio for NIFTY/BANKNIFTY (60s interval)

Redundancy Tiers:
    Candles: Upstox Intraday (PRIMARY if token available) → TradingView Premium → TradingView Public
    Breadth: NSE API → TradingView Screener (Nifty 50 constituents)
    PCR/OI: NSE v3 API → Trendlyne SQLite Database (with backfill support)

Usage:
    python tv_data_bridge.py
    # Starts WebSocket server on ws://localhost:6790

Dependencies:
    - websockets, pandas, numpy, requests
    - tradingview-screener, rookiepy
    - upstox-client (optional, for Level 3 fallback)
    - backfill_trendlyne (optional, for historical option chain)

Author: Mahesh
Version: 2.0 (Multi-Source Redundancy)
"""

import time
import json
import asyncio
import websockets
import pandas as pd
from datetime import datetime
from tradingview_screener import Query, col
from NSEAPICLient import NSEHistoricalAPI
from SymbolMaster import MASTER as SymbolMaster

# Yahoo Finance Import (Level 3 Fallback)
try:
    import yfinance as yf
    YAHOO_AVAILABLE = True
except ImportError:
    YAHOO_AVAILABLE = False
    print("[WARN] yfinance not found. Level 3 Candle Fallback disabled.")

try:
    from backfill_trendlyne import DB as TrendlyneDB, fetch_live_snapshot
except ImportError:
    TrendlyneDB = None
    fetch_live_snapshot = None
    print("[WARN] could not import backfill_trendlyne. Option chain data will be missing.")

# Upstox SDK Imports
try:
    import upstox_client
    import config
    UPSTOX_AVAILABLE = True
except ImportError:
    UPSTOX_AVAILABLE = False
    print("[WARN] Upstox SDK or config not found. Level 3 Fallback disabled.")
# Upstox SDK Imports
try:
    import upstox_client
    import config
    UPSTOX_AVAILABLE = True
except ImportError:
    UPSTOX_AVAILABLE = False
    print("[WARN] Upstox SDK or config not found. Level 3 Fallback disabled.")
# Configuration
# Adding more comprehensive list of symbols for scanning
SYMBOLS = [
    'RELIANCE', 'SBIN', 'ADANIENT', 'NIFTY', 'BANKNIFTY', 
    'HDFCBANK', 'ICICIBANK', 'INFY', 'TCS', 'BHARTIARTL', 
    'ITC', 'KOTAKBANK', 'HINDUNILVR', 'LT', 'AXISBANK', 
    'MARUTI', 'SUNPHARMA', 'TITAN', 'ULTRACEMCO', 'WIPRO',
    'BAJFINANCE', 'ASIANPAINT', 'HCLTECH', 'NTPC', 'POWERGRID'
]
PORT = 8765

class TVCandleBridge:
    def __init__(self, symbols):
        self.symbols = symbols
        self.nse = NSEHistoricalAPI()
        self.tickers = [f"NSE:{s}" for s in symbols]
        self.clients = set()
        self.pcr_data = {"NIFTY": 1.0, "BANKNIFTY": 1.0}
    async def broadcast_option_chain(self):
        """
        Periodically broadcasts Option Chain snapshots from Trendlyne DB.
        Also triggers a live fetch-and-save to ensure history is recorded.
        """
        loop = asyncio.get_running_loop()
        while True:
            if TrendlyneDB and self.clients and fetch_live_snapshot:
                for sym in ["NIFTY", "BANKNIFTY"]:
                    try:
                        # run_in_executor to avoid blocking main loop with requests
                        # fetch_live_snapshot saves to DB and returns the chain
                        chain = await loop.run_in_executor(None, fetch_live_snapshot, sym)
                        
                        if chain:
                            # Map to internal format
                            full_sym = "NSE_INDEX|Nifty 50" if sym == "NIFTY" else "NSE_INDEX|Nifty Bank"
                            msg = {
                                "type": "option_chain",
                                "symbol": full_sym,
                                "timestamp": int(time.time() * 1000),
                                "data": chain
                            }
                            await asyncio.gather(*[client.send(json.dumps(msg)) for client in self.clients], return_exceptions=True)
                    except Exception as e:
                        print(f"[OCR ERROR] {sym}: {e}")
            
            await asyncio.sleep(60) # Update chain every 60 seconds (1-min resolution)

    async def broadcast_market_breadth(self):
        """
        Fetches live Adv/Dec from NSE, saves to DB, and broadcasts.
        """
        while True:
            breadth_msg = None
            try:
                data = self.nse.get_market_breadth()
                if data and 'advance' in data:
                    counts = data['advance'].get('count', {})
                    
                    # Convert to our standardized format
                    breadth_msg = {
                        "type": "market_breadth",
                        "timestamp": int(time.time() * 1000),
                        "data": {
                            "advances": counts.get('Advances', 0),
                            "declines": counts.get('Declines', 0),
                            "unchanged": counts.get('Unchange', 0),
                            "total": counts.get('Total', 0),
                            "sectors": {} 
                        }
                    }
            except Exception as e:
                print(f"[BREADTH NSE ERROR] {e}. Trying TV Fallback...")
                try:
                    # Fallback: Calculate from Nifty 50 stocks using TV Screener
                    scanner = Query().select('name', 'change').limit(50).get_scanner_data()
                    rows = scanner[1]
                    adv = len(rows[rows['change'] > 0])
                    dec = len(rows[rows['change'] < 0])
                    unc = len(rows[rows['change'] == 0])
                    breadth_msg = {
                        "type": "market_breadth",
                        "timestamp": int(time.time() * 1000),
                        "data": {
                            "advances": adv, "declines": dec, "unchanged": unc, "total": 50,
                            "sectors": {"source": "TV_FALLBACK"}
                        }
                    }
                except Exception as tve:
                    print(f"[CRITICAL] Breadth TV Fallback failed: {tve}")

            if breadth_msg:
                # Persist Snapshot
                if TrendlyneDB:
                    d_str = datetime.now().strftime("%Y-%m-%d")
                    ts_str = datetime.now().strftime("%H:%M")
                    TrendlyneDB.save_breadth(d_str, ts_str, breadth_msg['data'])

                # Broadcast to CLIENTS
                if self.clients:
                    await asyncio.gather(*[client.send(json.dumps(breadth_msg)) for client in self.clients], return_exceptions=True)
            
            await asyncio.sleep(30) # Poll breadth every 30 seconds

    async def update_pcr(self):
        """Periodically updates PCR data from NSE Live API or Trendlyne DB."""
        while True:
            try:
                for sym in ["NIFTY", "BANKNIFTY"]:
                    # Try NSE Live API first (v3)
                    try:
                        data = self.nse.get_option_chain_v3(sym, indices=True)
                        if data and 'records' in data:
                            filtered = data.get('filtered', {})
                            if filtered:
                                ce_oi = filtered.get('CE', {}).get('totOI', 0)
                                pe_oi = filtered.get('PE', {}).get('totOI', 0)
                                if ce_oi > 0:
                                    self.pcr_data[sym] = round(pe_oi / ce_oi, 2)
                                    print(f"[PCR LIVE] {sym}: {self.pcr_data[sym]} (NSE)")
                                    
                                    # Save to daily history if near EOD
                                    now = datetime.now()
                                    if now.hour == 15 and now.minute >= 25:
                                        if TrendlyneDB:
                                            TrendlyneDB.save_daily_stats(sym, now.strftime("%Y-%m-%d"), 
                                                                       self.pcr_data[sym], ce_oi, pe_oi)
                                    
                                    continue # Skip Trendlyne if NSE is successful
                    except Exception as nse_e:
                        print(f"[PCR NSE ERROR] {sym}: {nse_e}")

                    # Fallback to Trendlyne DB
                    if TrendlyneDB:
                        agg = TrendlyneDB.get_latest_aggregates(sym)
                        if agg:
                            self.pcr_data[sym] = agg['pcr']
                            print(f"[PCR DB] {sym}: {agg['pcr']} (Trendlyne)")
                    else:
                        self.pcr_data[sym] = 1.0 # Default
            except Exception as e:
                print(f"PCR Update Error: {e}")
            await asyncio.sleep(60) # check every minute

    async def fetch_candles(self):
        """
        Fetches 1-minute and 5-minute candle snapshots with intelligent prioritization.
        
        Priority Order:
            1. Upstox Intraday API (if token available) - Most reliable for current day
            2. TradingView Premium (with cookies) - Fallback
            3. TradingView Public (screener) - Final fallback
        """
        # PRIMARY: Upstox (if available and token valid)
        if UPSTOX_AVAILABLE and hasattr(config, 'ACCESS_TOKEN') and config.ACCESS_TOKEN:
            try:
                result = self.fetch_candles_upstox_primary()
                if result:  # If Upstox returned data successfully
                    return result
                print("[UPSTOX PRIMARY] No data returned, falling back to TradingView...")
            except Exception as e:
                print(f"[UPSTOX PRIMARY ERROR] {e}. Trying TradingView...")
        
        # FALLBACK LEVEL 1: TradingView Premium (with cookies)
        try:
            return self._fetch_candles_logic(use_cookies=True)
        except Exception as e:
            print(f"[TV PRIMARY ERROR] {e}. Trying Lightweight Fallback (No Cookies)...")
            try:
                # FALLBACK LEVEL 2: TradingView Public (no cookies)
                return self._fetch_candles_logic(use_cookies=False)
            except Exception as fe:
                print(f"[TV FALLBACK ERROR] {fe}. Trying Yahoo Finance...")
                
                # FALLBACK LEVEL 3: Yahoo Finance
                try:
                    return self._fetch_candles_yahoo()
                except Exception as ye:
                    print(f"[YAHOO FALLBACK ERROR] {ye}. All sources exhausted.")
                    return []

    def _fetch_candles_yahoo(self):
        """FALLBACK LEVEL 3: Yahoo Finance (most basic, 15m delay often)."""
        if not YAHOO_AVAILABLE:
            return []
            
        print("[FALLBACK] Fetching candles from Yahoo Finance...")
        candles = []
        ts = int(time.time() // 60 * 60 * 1000)
        
        # Batch fetching is better but iterating for now to map symbols
        # Format: RELIANCE.NS, ^NSEI (Nifty), ^NSEBANK (Bank Nifty)
        
        for sym in self.symbols:
            y_sym = f"{sym}.NS"
            if sym == "NIFTY": y_sym = "^NSEI"
            if sym == "BANKNIFTY": y_sym = "^NSEBANK"
            
            try:
                ticker = yf.Ticker(y_sym)
                # Fetch 1 day, 1m interval
                df = ticker.history(period="1d", interval="1m")
                if df.empty: continue
                
                last_row = df.iloc[-1]
                
                ltp = float(last_row['Close'])
                c_data = {
                    "symbol": sym, "timestamp": ts,
                    "1m": {
                        "open": float(last_row['Open']), 
                        "high": float(last_row['High']), 
                        "low": float(last_row['Low']), 
                        "close": ltp, 
                        "volume": int(last_row['Volume']),
                        "vwap": ltp 
                    },
                    "5m": { # Yahoo raw doesn't give 5m easily mixed, so replicate 1m or approx
                         "open": float(last_row['Open']), "high": float(last_row['High']), 
                         "low": float(last_row['Low']), "close": ltp, "volume": int(last_row['Volume'])
                    },
                    "pcr": self.pcr_data.get(sym, 1.0)
                }
                candles.append(c_data)
            except:
                pass
                
        return candles

    def fetch_candles_upstox_primary(self):
        """PRIMARY: Fetch latest intraday candles using Upstox HistoryV3 API."""
        if not UPSTOX_AVAILABLE or not config.ACCESS_TOKEN:
            print("[CRITICAL] Upstox Fallback unavailable (No Token/SDK).")
            return []
        
        upstox_candles = []
        try:
            configuration = upstox_client.Configuration()
            configuration.access_token = config.ACCESS_TOKEN
            api_client = upstox_client.ApiClient(configuration)
            history_api = upstox_client.HistoryV3Api(api_client) # Use HistoryV3Api
            
            ts = int(time.time() // 60 * 60 * 1000)
            
            for sym in self.symbols:
                u_key = SymbolMaster.get_upstox_key(sym)
                if not u_key: continue
                
                try:
                    # Fetch Intraday Data (Current Day)
                    # Verified Signature: (instrument_key, unit="minutes", interval="1")
                    response = history_api.get_intra_day_candle_data(u_key, "minutes", "1")
                    
                    if response and hasattr(response, 'data') and hasattr(response.data, 'candles'):
                        candles = response.data.candles
                        if not candles: continue
                        
                        # Use the latest candle
                        # Format: [timestamp, open, high, low, close, volume, oi]
                        # Verify sort order: Upstox typically returns ascending (last item is latest) or descending?
                        # Usually logic: check timestamp of index 0 vs -1.
                        # Assuming index 0 is latest for now based on typical API, but safe to check.
                        # Actually standard for Upstox V3 is reversed? Let's check dates.
                        # Safest: Sort by timestamp
                        
                        sorted_candles = sorted(candles, key=lambda x: x[0], reverse=True)
                        last_candle = sorted_candles[0]
                        
                        # Parse
                        # [ "2024-01-01T09:15:00+05:30", 100.0, 105.0, 99.0, 102.0, 5000, 0]
                        # Timestamp likely iso string in V3 response
                        
                        ltp = float(last_candle[4]) # Close
                        op = float(last_candle[1])
                        hi = float(last_candle[2])
                        lo = float(last_candle[3])
                        vol = int(last_candle[5])
                        
                        # Create Candle Packet
                        c_data = {
                            "symbol": sym, "timestamp": ts,
                            "1m": {
                                "open": op, "high": hi, "low": lo, "close": ltp, "volume": vol,
                                "vwap": ltp # Approximation
                            },
                            "5m": { 
                                 "open": op, "high": hi, "low": lo, "close": ltp, "volume": vol
                            },
                            "pcr": self.pcr_data.get(sym, 1.0)
                        }
                        upstox_candles.append(c_data)

                except Exception as inner_e:
                     # print(f"[UPSTOX INNER ERROR] {sym}: {inner_e}")
                     continue
                
            if upstox_candles:
                 print(f"[UPSTOX PRIMARY] Recovered {len(upstox_candles)} symbols.")
            return upstox_candles

        except Exception as e:
            print(f"[CRITICAL] Upstox Primary Failed: {e}")
            return []

    def _fetch_candles_logic(self, use_cookies=True):
        """Standardized candle fetching logic for both primary and fallback paths."""
        scanner_query = Query().select(
            'name', 
            'open|1', 'high|1', 'low|1', 'close|1', 'volume|1',
            'open|5', 'high|5', 'low|5', 'close|5', 'volume|5',
            'VWAP|1'
        ).set_tickers(*self.tickers).get_scanner_data(cookies=cookies if use_cookies else None)
        
        candles = []
        if len(scanner_query) > 1:
            ts = int(time.time() // 60 * 60 * 1000) # Minute-aligned timestamp
            for _, row in scanner_query[1].iterrows():
                sym = row['name'].split(':')[-1]
                
                candle_data = {
                    "symbol": sym,
                    "timestamp": ts,
                    "1m": {
                        "open": row['open|1'],
                        "high": row['high|1'],
                        "low": row['low|1'],
                        "close": row['close|1'],
                        "volume": row['volume|1'],
                        "vwap": row.get('VWAP|1', row['close|1']), # Fallback
                    },
                    "5m": {
                        "open": row['open|5'],
                        "high": row['high|5'],
                        "low": row['low|5'],
                        "close": row['close|5'],
                        "volume": row['volume|5'],
                    },
                    "pcr": self.pcr_data.get(sym, 1.0)
                }
                candles.append(candle_data)
        return candles

    async def broadcast(self):
        """Continuously fetches and broadcasts 1-min candles."""
        print("Starting candle broadcast loop...")
        while True:
            if self.clients:
                data = await self.fetch_candles()
                if data:
                    message = json.dumps({"type": "candle_update", "data": data})
                    # Use gather to send to all clients
                    await asyncio.gather(*[client.send(message) for client in self.clients], return_exceptions=True)
            
            # Since we want "1 min candles", we poll every 10 seconds 
            # to catch the update as soon as the candle matures or price changes
            await asyncio.sleep(10)

    async def handler(self, websocket, path=None):
        self.clients.add(websocket)
        print(f"Client connected: {websocket.remote_address}. Total: {len(self.clients)}")
        try:
            async for message in websocket:
                pass
        except websockets.ConnectionClosed:
            pass
        finally:
            self.clients.remove(websocket)
            print(f"Client disconnected. Total: {len(self.clients)}")

    async def main_loop(self):
        """Starts the WebSocket server and concurrent tasks."""
        async with websockets.serve(self.handler, "localhost", PORT):
            print(f"TV Candle Bridge started on ws://localhost:{PORT}")
            # Run broadcast and update_pcr concurrently
            await asyncio.gather(
                self.broadcast(),
                self.update_pcr(),
                self.broadcast_option_chain(),
                self.broadcast_market_breadth()
            )

    def run(self):
        """Entry point for the bridge."""
        try:
            asyncio.run(self.main_loop())
        except KeyboardInterrupt:
            print("\nServer shutting down...")
        except Exception as e:
            print(f"Fatal error: {e}")

if __name__ == "__main__":
    bridge = TVCandleBridge(SYMBOLS)
    bridge.run()
