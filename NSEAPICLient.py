import requests
import time
import json

class NSEHistoricalAPI:
    def __init__(self):
        self.base_url = "https://www.nseindia.com"
        self.headers = {
            # Use a robust User-Agent to mimic a real browser
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
            "Accept": "*/*",
            "Accept-Language": "en-US,en;q=0.9",
            # This referer works for general reports area
            "Referer": "www.nseindia.com",
            "Connection": "keep-alive"
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def _init_session(self):
        """Step 1: Hit homepage to get valid cookies if session is new."""
        # Check if we already have session cookies
        if not self.session.cookies:
            try:
                self.session.get(self.base_url, timeout=10)
                # print("Session initialized with new cookies.")
            except Exception as e:
                print(f"Failed to initialize session: {e}")

    def _make_get_request(self, url, params=None):
        """Helper method for making authenticated GET requests."""
        self._init_session()
        time.sleep(0.5) # Be kind to their servers
        try:
            response = self.session.get(url, params=params, timeout=10)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            print(f"HTTP error: {e.response.status_code} - {e.response.text}")
        except requests.exceptions.RequestException as e:
            print(f"Request failed: {e}")
        return None

    def get_historical_options(self, symbol, from_date, to_date, expiry, option_type="CE", instrument="OPTSTK"):
        """Fetches historical options data for a specific range/expiry."""
        url = f"{self.base_url}/api/historicalOR/foCPV"
        params = {
            "from": from_date,
            "to": to_date,
            "instrumentType": instrument,
            "symbol": symbol,
            "year": from_date.split('-')[-1],
            "expiryDate": expiry,
            "optionType": option_type
        }
        return self._make_get_request(url, params=params)

    # --- New APIs Added ---

    def get_available_symbols(self, instrument_type):
        """
        Fetches the list of available symbols for a given instrument type (e.g., OPTSTK, FUTIDX).
        """
        url = f"{self.base_url}/api/historicalOR/meta/foCPV/symbolv2"
        params = {"instrument": instrument_type}
        print(f"\nFetching symbols for {instrument_type}...")
        return self._make_get_request(url, params=params)
    
    def get_option_chain_v3(self, symbol, indices=True):
        """
        Fetches the live option chain (v3) which includes total OI and expiry dates.
        URL: https://www.nseindia.com/api/option-chain-v3
        """
        instrument_type = "Indices" if indices else "Equities"
        url = f"{self.base_url}/api/option-chain-v3"
        params = {"type": instrument_type, "symbol": symbol}
        
        headers = self.headers.copy()
        headers["Referer"] = f"{self.base_url}/get-quotes/derivatives?symbol={symbol}"
        self.session.headers.update(headers)
        
        return self._make_get_request(url, params=params)

    def get_market_breadth(self):
        """
        Fetches market advances, declines, and unchanged counts.
        URL: https://www.nseindia.com/api/live-analysis-advance
        """
        url = f"{self.base_url}/api/live-analysis-advance"
        # The referer might need to be specific for live data
        headers = self.headers.copy()
        headers["Referer"] = f"{self.base_url}/market-data/live-equity-market"
        self.session.headers.update(headers)
        return self._make_get_request(url)

    def get_expiry_dates(self, instrument_type, symbol, year):
        """
        Fetches available expiry dates for a specific symbol, instrument, and year.
        """
        url = f"{self.base_url}/api/historicalOR/meta/foCPV/expireDts"
        params = {
            "instrument": instrument_type,
            "symbol": symbol,
            "year": year
        }
        print(f"\nFetching expiry dates for {symbol} ({year})...")
        return self._make_get_request(url, params=params)


# --- Example Usage ---
if __name__ == "__main__":
    nse_api = NSEHistoricalAPI()

    # 1. Fetch available stock option symbols (OPTSTK)
    stock_symbols = nse_api.get_available_symbols(instrument_type="OPTSTK")
    if stock_symbols:
        print(f"Found {len(stock_symbols)} stock symbols. First 5: {stock_symbols[:5]}")

    # 2. Fetch available index futures symbols (FUTIDX)
    index_futures_symbols = nse_api.get_available_symbols(instrument_type="FUTIDX")
    if index_futures_symbols:
         # Note: Symbols are often returned as simple strings in a list
        print(f"Found {len(index_futures_symbols)} index future symbols. First 5: {index_futures_symbols[:5]}")

    # 3. Fetch expiry dates for a specific symbol and year (e.g., ABB in 2025)
    abb_expiries = nse_api.get_expiry_dates(
        instrument_type="OPTSTK", 
        symbol="ABB", 
        year="2025"
    )
    if abb_expiries:
        print(f"Expiry dates for ABB in 2025: {abb_expiries}")

    # 4. Fetch expiry dates for a major index
    banknifty_expiries = nse_api.get_expiry_dates(
        instrument_type="FUTIDX",
        symbol="BANKNIFTY",
        year="2025"
    )
    if banknifty_expiries:
        print(f"Expiry dates for BANKNIFTY in 2025: {banknifty_expiries}")


# --- Usage ---
if __name__ == "__main__":
    nse = NSEHistoricalAPI()
    
    data = nse.get_historical_options(
        symbol="RELIANCE",
        from_date="27-12-2025",
        to_date="03-01-2026",
        expiry="30-DEC-2025",
        option_type="CE"
    )
    
    if data:
        print(data)
