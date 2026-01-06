# Python Bridge <-> Java Engine Interface Specification

This document defines the WebSocket communication protocol between the Python Data Bridge (`tv_data_bridge.py`) and the Java Trading Engine (`TVMarketDataStreamer.java`).

## Connection Details

*   **Protocol**: WebSocket (WS)
*   **Host**: `localhost`
*   **Port**: `8765`
*   **URL**: `ws://localhost:8765`
*   **Direction**: Python Bridge (Server) -> Java Engine (Client)
*   **Message Format**: JSON String

---

## Message Types

The bridge broadcasts three distinct message types. The `type` field in the root JSON object determines the payload structure.

### 1. Candle Update (`candle_update`)

Broadcasts 1-minute and 5-minute OHLCV candles for all subscribed symbols, plus the latest PCR and VWAP.

*   **Frequency**: Every 10 seconds (Broadcasting 1-minute snapshots)
*   **Structure**:

```json
{
  "type": "candle_update",
  "data": [
    {
      "symbol": "RELIANCE",           // Trading Symbol without Exchange Prefix
      "timestamp": 1704519300000,     // Epoch Milliseconds (Minute Aligned)
      "1m": {
        "open": 2500.0,
        "high": 2505.0,
        "low": 2498.0,
        "close": 2502.5,
        "volume": 15000,
        "vwap": 2501.2                // Volume Weighted Average Price
      },
      "5m": {
        "open": 2490.0,
        "high": 2510.0,
        "low": 2488.0,
        "close": 2502.5,
        "volume": 75000
      },
      "pcr": 0.85                     // Put-Call Ratio (if available, else 1.0)
    },
    // ... more symbols
  ]
}
```

*   **Java Mapping**: `TVMarketDataStreamer.processCandle()`
    *   Maps `symbol` to internal keys: `NIFTY` -> `NSE_INDEX|Nifty 50`, `RELIANCE` -> `NSE_EQ|RELIANCE`.

### 2. Option Chain Update (`option_chain`)

Broadcasts the full Option Chain (OI and Greeks) for indices (NIFTY, BANKNIFTY). Source is Upstox (Primary) or Trendlyne (Fallback).

*   **Frequency**: Every 60 seconds
*   **Structure**:

```json
{
  "type": "option_chain",
  "symbol": "NSE_INDEX|Nifty 50",     // Full Internal Instrument Key
  "timestamp": 1704519360000,
  "data": [
    {
      "strike": 21500.0,
      "call_oi": 1500000,
      "put_oi": 2000000,
      "call_oi_chg": 50000,           // Change in OI since previous day
      "put_oi_chg": -10000
    },
    {
      "strike": 21550.0,
      "call_oi": 800000,
      "put_oi": 100000,
      "call_oi_chg": 2000,
      "put_oi_chg": 500
    }
    // ... all strikes
  ]
}
```

*   **Java Mapping**: `TVMarketDataStreamer.processOptionChain()`
    *   Updates `OptionChainProvider`.

### 3. Market Breadth (`market_breadth`)

Broadcasts the overall market sentiment stats (Advances vs. Declines). Source is NSE API (Primary) or TradingView Nifty 50 calc (Fallback).

*   **Frequency**: Every 30 seconds
*   **Structure**:

```json
{
  "type": "market_breadth",
  "timestamp": 1704519330000,
  "data": {
    "advances": 35,
    "declines": 15,
    "unchanged": 0,
    "total": 50,
    "sectors": {}                     // Optional sector-wise breakdown
  }
}
```

*   **Java Mapping**: `TVMarketDataStreamer.processMarketBreadth()`
    *   Updates `MarketBreadthEngine`.

---

## Data Source Redundancy Hierarchy

The Python bridge abstracts the source complexity from the Java engine. The attributes in the JSON remain consistent regardless of the upstream source.

| Data Type | Primary Source | Fallback 1 | Fallback 2 | Fallback 3 |
| :--- | :--- | :--- | :--- | :--- |
| **Candles** | **Upstox API** (Real-time) | TradingView Premium | TradingView Public | Yahoo Finance (Delayed) |
| **Option Chain** | **Upstox API** (Real-time) | Trendlyne (Live Fetch) | - | - |
| **Breadth** | **NSE API** (Live) | TradingView Nifty 50 Calc | - | - |

## Error Handling

*   **Disconnection**: The Java client (`TVMarketDataStreamer`) attempts to reconnect every 5 seconds if the WebSocket connection is lost.
*   **Missing Fields**: Java parser uses safe defaults (e.g., `pcr` defaults to `1.0`, `vwap` defaults to `close`).
*   **Unknown Types**: Unknown message `type` values are logged and ignored, preventing crashes.
