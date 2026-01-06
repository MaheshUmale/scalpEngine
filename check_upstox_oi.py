
import upstox_client
import config
from datetime import datetime, timedelta

def check_upstox_oi():
    print("Initializing Upstox Client...")
    configuration = upstox_client.Configuration()
    configuration.access_token = config.ACCESS_TOKEN
    api_client = upstox_client.ApiClient(configuration)
    
    # Use OptionApi to get option chain
    # Note: Upstox SDK usually provides get_option_chain or get_full_market_quote
    # Let's try to get instrument keys for Options first or use the correct API
    # Since we can't search for option keys easily without downloading the full master, 
    # we will use the 'get_option_chain' endpoint if available in SDK structure,
    # otherwise we might need to rely on 'get_full_market_quote' for KNOWN keys.
    
    # However, let's try the specialized Option Chain API often available in v3
    
    try:
        # Check if 'OptionApi' is available
        if hasattr(upstox_client, 'OptionsApi'):
             api_instance = upstox_client.OptionsApi(api_client)
             instrument_key = "NSE_INDEX|Nifty 50" # Nifty 50 underlying
             expiry_date = (datetime.now() + timedelta(days=2)).strftime("%Y-%m-%d") # Approximation
             
             # We need a valid expiry date to call this API
             # Let's get contracts for underlying to find expiry first?
             # Or just try to get the 'option chain' for the instrument key
             
             print(f"Fetching Option Chain for {instrument_key}...")
             api_response = api_instance.get_put_call_option_chain(
                 instrument_key,
                 expiry_date
             )
             print(api_response)
             
        else:
            # Fallback for SDK versions that might structure it differently
            # Try getting Quotes for an underlying, does it have OI?
            market_quote_api = upstox_client.MarketQuoteApi(api_client)
            instrument_key = "NSE_INDEX|Nifty 50"
            response = market_quote_api.get_full_market_quote(instrument_key, "v2") 
            # Note: Index quote usually has 0 OI.
            # We need an OPTION instrument to see OI.
            
            print("SDK does not seem to have high-level 'get_put_call_option_chain' helper or we need exact expiry.")
            print("Attempting to check metadata in `collect_backtest_data.py` logic which gets candles.")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    check_upstox_oi()
