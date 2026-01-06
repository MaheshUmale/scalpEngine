import re
import pandas as pd
import sqlite3
import argparse

def parse_log_file(log_path):
    signals = []

    # Updated pattern for the new log format
    signal_pattern = re.compile(r"SCALP SIGNAL \[(.*?)\] for (.*?): (.*?) \| Entry: ([\d\.]+) \| Stop: ([\d\.]+) \| Take Profit: ([\d\.]+) \| Position Size: ([\d\.]+)")

    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        for line in f:
            match = signal_pattern.search(line)
            if match:
                signals.append({
                    'Strategy': match.group(1),
                    'Symbol': match.group(2),
                    'Side': match.group(3),
                    'Entry': float(match.group(4)),
                    'SL': float(match.group(5)),
                    'TP': float(match.group(6)),
                    'PositionSize': float(match.group(7)),
                })

    return signals

def load_candle_data(db_path, date):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(f"SELECT * FROM backtest_candles WHERE date = '{date}'", conn)
    conn.close()
    return df

def simulate_trades(signals, candles_df):
    exits = []

    for signal in signals:
        # Get the candles for the symbol after the signal
        symbol_candles = candles_df[candles_df['symbol'] == signal['Symbol']].sort_values('timestamp')

        # Find the candle that generated the signal
        entry_candle_index = symbol_candles[symbol_candles['close'] == signal['Entry']].index.min()

        if pd.isna(entry_candle_index):
            continue

        trade_candles = symbol_candles[symbol_candles.index > entry_candle_index]

        pnl = 0
        reason = "NO_EXIT"
        exit_price = 0

        for _, candle in trade_candles.iterrows():
            if signal['Side'] == 'LONG':
                if candle['high'] >= signal['TP']:
                    pnl = (signal['TP'] - signal['Entry']) * signal['PositionSize']
                    reason = "TP_HIT"
                    exit_price = signal['TP']
                    break
                elif candle['low'] <= signal['SL']:
                    pnl = (signal['SL'] - signal['Entry']) * signal['PositionSize']
                    reason = "SL_HIT"
                    exit_price = signal['SL']
                    break
            else: # SHORT
                if candle['low'] <= signal['TP']:
                    pnl = (signal['Entry'] - signal['TP']) * signal['PositionSize']
                    reason = "TP_HIT"
                    exit_price = signal['TP']
                    break
                elif candle['high'] >= signal['SL']:
                    pnl = (signal['Entry'] - signal['SL']) * signal['PositionSize']
                    reason = "SL_HIT"
                    exit_price = signal['SL']
                    break

        exits.append({
            'Side': signal['Side'],
            'Symbol': signal['Symbol'],
            'ExitPrice': exit_price,
            'Reason': reason,
            'PnL': pnl,
            'GateKey': signal['Strategy'] # Use strategy name as GateKey
        })

    return exits


def analyze_results(signals, exits):
    print(f"Total Signals: {len(signals)}")
    print(f"Total Exits: {len(exits)}")

    # Convert to DF for easier analysis
    df_exits = pd.DataFrame(exits)

    if df_exits.empty:
        print("No trades completed.")
        return

    df_exits['Gate'] = df_exits['GateKey']

    # 1. Overall Performance
    total_pnl = df_exits['PnL'].sum()
    win_trades = df_exits[df_exits['PnL'] > 0]
    loss_trades = df_exits[df_exits['PnL'] <= 0]
    win_rate = len(win_trades) / len(df_exits) * 100 if len(df_exits) > 0 else 0

    print("\n" + "="*40)
    print("OVERALL PERFORMANCE")
    print("="*40)
    print(f"Total PnL: {total_pnl:.2f}")
    print(f"Win Rate: {win_rate:.2f}% ({len(win_trades)}W / {len(loss_trades)}L)")
    print(f"Avg PnL per Trade: {df_exits['PnL'].mean():.2f}")

    # 2. Performance by Gate
    print("\n" + "="*40)
    print("PERFORMANCE BY STRATEGY (GATE)")
    print("="*40)
    gate_stats = df_exits.groupby('Gate')['PnL'].agg(['count', 'sum', 'mean', 'min', 'max'])
    gate_stats['WinRate'] = df_exits.groupby('Gate').apply(lambda x: (x['PnL'] > 0).sum() / len(x) * 100)
    print(gate_stats.sort_values(by='sum', ascending=False))

    # 3. Performance by Exit Reason
    print("\n" + "="*40)
    print("EXIT REASON ANALYSIS")
    print("="*40)
    reason_stats = df_exits.groupby('Reason')['PnL'].agg(['count', 'sum', 'mean'])
    print(reason_stats)

    # 4. Weak Spots Detection
    print("\n" + "="*40)
    print("WEAK SPOTS IDENTIFICATION")
    print("="*40)

    # Strategies with < 40% Win Rate
    weak_strategies = gate_stats[gate_stats['WinRate'] < 40]
    if not weak_strategies.empty:
        print("!! LOW WIN RATE STRATEGIES (< 40%) !!")
        print(weak_strategies[['count', 'WinRate', 'sum']])
    else:
        print("No strategies below 40% win rate.")

    # High Loss Strategies
    loss_strategies = gate_stats[gate_stats['sum'] < -5000]
    if not loss_strategies.empty:
        print("\n!! HIGH LOSS STRATEGIES (< -5000 Total PnL) !!")
        print(loss_strategies[['count', 'sum']])

    # 5. Context correlation (if time is available)
    # We would need to map exits back to entries to get time.
    # For now, let's just look at the raw data we have.

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Analyze backtest results")
    parser.add_argument('--date', type=str, default='2026-01-05', help='Target date (YYYY-MM-DD)')
    args = parser.parse_args()

    log_file = "backtest_java.log"
    db_file = "backtest_data.db"

    print(f"Analyzing {log_file}...")
    signals = parse_log_file(log_file)

    print(f"Loading candle data from {db_file}...")
    candles = load_candle_data(db_file, args.date)

    print("Simulating trades...")
    exits = simulate_trades(signals, candles)

    analyze_results(signals, exits)
