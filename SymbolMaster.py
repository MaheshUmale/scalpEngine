"""
SymbolMaster - Centralized Instrument Key Resolution

This singleton class provides a unified interface for resolving instrument symbols
across different data providers (TradingView, NSE, Upstox).

Key Features:
- Downloads and caches Upstox NSE instrument master (8,792+ instruments)
- Bidirectional mapping: Symbol ↔ Instrument Key
- Special handling for index instruments (NIFTY, BANKNIFTY)

Usage:
    from SymbolMaster import MASTER
    
    MASTER.initialize()  # Downloads instrument master (one-time)
    
    # Resolve symbol to Upstox key
    key = MASTER.get_upstox_key("RELIANCE")  # Returns: NSE_EQ|INE002A01018
    
    # Reverse lookup
    symbol = MASTER.get_ticker_from_key(key)  # Returns: RELIANCE

Architecture:
    - Singleton pattern ensures single download per session
    - Filters for NSE_EQ and NSE_INDEX segments only
    - Caches mappings in-memory for O(1) lookups

Author: Mahesh
Version: 1.0
"""

import requests
import gzip
import io
import pandas as pd
import os

class SymbolMaster:
    _instance = None
    _mappings = {} # { "RELIANCE": "NSE_EQ|INE002A01018" }
    _reverse_mappings = {} # { "NSE_EQ|INE002A01018": "RELIANCE" }
    _initialized = False

    def __new__(cls):
        """Singleton pattern implementation."""
        if cls._instance is None:
            cls._instance = super(SymbolMaster, cls).__new__(cls)
        return cls._instance

    def initialize(self):
        """
        Downloads and parses the Upstox NSE instrument master.
        
        This method is idempotent - subsequent calls are no-ops.
        Attempts to load from disk cache first if download fails.
        
        Raises:
            Exception: If both download and cache load fail
        """
        if self._initialized:
            return
            
        cache_file = "upstox_instruments.json.gz"
        content = None
        
        # 1. Try to Download
        try:
            print("[SymbolMaster] Initializing Instrument Keys...")
            url = "https://assets.upstox.com/market-quote/instruments/exchange/NSE.json.gz"
            response = requests.get(url, timeout=60)
            response.raise_for_status()
            content = response.content
            
            # Save to cache
            with open(cache_file, "wb") as f:
                f.write(content)
            print(f"  ✓ Downloaded and cached to {cache_file}")
            
        except Exception as e:
            print(f"  [WARN] Download failed: {e}")
            # 2. Fallback to Disk Cache
            if os.path.exists(cache_file):
                print(f"  [INFO] Loading from disk cache: {cache_file}")
                with open(cache_file, "rb") as f:
                    content = f.read()
            else:
                print("  ✗ No disk cache available.")
                raise e
        
        # 3. Parse content
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(content)) as f:
                df = pd.read_json(f)
            
            # 2. Filter for Equities and Indices
            # Equities have lot_size=1 usually, Indices have specific names
            # Simplify: Just map by Trading Symbol -> Instrument Key
            
            # Create Fast Lookup
            # "RELIANCE" -> "NSE_EQ|INE..."
            # "NIFTY 50" -> "NSE_INDEX|Nifty 50"
            
            # Optimization: Filter for Equities and Indices using 'segment'
            # Segments found: 'NSE_EQ', 'NSE_INDEX'
            df_filtered = df[df['segment'].isin(['NSE_EQ', 'NSE_INDEX'])].copy()
            
            for _, row in df_filtered.iterrows():
                # Clean name: "Nifty 50" -> "NIFTY", "RELIANCE" -> "RELIANCE"
                # Some might be "RELIANCE-EQ" or similar, usually trading_symbol is clean for equities
                name = row['trading_symbol'].upper() 
                key = row['instrument_key'] 
                
                self._mappings[name] = key
                self._reverse_mappings[key] = name
                
                # Special Handling for Indices (trading_symbol might be specific)
                # For NSE_INDEX, name is often "Nifty 50", trading_symbol "Nifty 50"
                if row['segment'] == 'NSE_INDEX':
                    if row['name'] == "Nifty 50" or row['trading_symbol'] == "Nifty 50":
                        self._mappings["NIFTY"] = key
                    elif row['name'] == "Nifty Bank" or row['trading_symbol'] == "Nifty Bank":
                        self._mappings["BANKNIFTY"] = key

            print(f"[SymbolMaster] Loaded {len(self._mappings)} keys.")
            self._initialized = True
            
        except Exception as e:
            print(f"[SymbolMaster] Initialization Failed: {e}")

    def get_upstox_key(self, symbol):
        """
        Resolves a trading symbol to its Upstox instrument key.
        
        Args:
            symbol (str): Trading symbol (e.g., 'RELIANCE', 'NIFTY', 'SBIN')
            
        Returns:
            str: Upstox instrument key (e.g., 'NSE_EQ|INE002A01018') or None if not found
            
        Example:
            >>> MASTER.get_upstox_key('RELIANCE')
            'NSE_EQ|INE002A01018'
        """
        if not self._initialized:
            self.initialize()
        
        # 1. Direct Match
        if symbol.upper() in self._mappings:
            return self._mappings[symbol.upper()]
        
        # 2. Try adding -EQ extension common in some systems
        # N/A for now
        
        return None

    def get_ticker_from_key(self, key):
        """
        Reverse lookup: Upstox instrument key to trading symbol.
        
        Args:
            key (str): Upstox instrument key (e.g., 'NSE_EQ|INE002A01018')
            
        Returns:
            str: Trading symbol or the original key if not found
            
        Example:
            >>> MASTER.get_ticker_from_key('NSE_INDEX|Nifty 50')
            'NIFTY'
        """
        if not self._initialized:
            self.initialize()
        return self._reverse_mappings.get(key, key)

# Singleton Instance
MASTER = SymbolMaster()
