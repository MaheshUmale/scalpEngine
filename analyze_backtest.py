import re
import pandas as pd
import datetime

def parse_log_file(log_path):
    signals = []
    executions = []
    exits = []

    # Updated Regex patterns based on actual log observation
    # It appears the logger prepends timestamp and thread info:
    # "SCALP SIGNAL [Gate]: Symbol Entry: ... "
    # But previous tool output showed "com.trading.hf.ScalpingSignalEngine - SCALP SIGNAL [CRUSH_L]:"
    # It's possible the rest of the message is on the same line.
    
    # Try a more robust pattern
    signal_pattern = re.compile(r"SCALP SIGNAL \[(.*?)\]: (.*?) Entry: ([\d\.]+) SL: ([\d\.]+) TP: ([\d\.]+)")
    
    # "AUTO-EXECUTED [{}]: {} Qty: {} @ {} SL: {} TP: {} (Gate: {})"
    exec_pattern = re.compile(r"AUTO-EXECUTED \[(.*?)\]: (.*?) Qty: (\d+) @ ([\d\.]+) SL: ([\d\.]+) TP: ([\d\.]+) \(Gate: (.*?)\)")
    
    # "AUTO-EXIT [{}]: {} @ {} Reason: {} PnL: {} (Gate Refreshed: {})"
    # Note: PnL might be negative
    exit_pattern = re.compile(r"AUTO-EXIT \[(.*?)\]: (.*?) @ ([\d\.]+) Reason: (.*?) PnL: ([\-\d\.]+) \(Gate Refreshed: (.*?)\)")

    # File is likely UTF-16LE per previous errors
    with open(log_path, 'r', encoding='utf-16', errors='ignore') as f:
        iterator = iter(f)
        for line in iterator:
            if "SCALP SIGNAL" in line:
                # Format: com.trading.hf.ScalpingSignalEngine - SCALP SIGNAL [Gate]:\n Symbol Entry: ...
                # We need to read the next line to get details.
                gate_match = re.search(r"SCALP SIGNAL \[(.*?)\]:", line)
                if gate_match:
                    gate = gate_match.group(1)
                    
                    try:
                        details_line = next(iterator)
                        # Pattern: Symbol Entry: 996.5 SL: ...
                        # Example: NSE_EQ|HDFCBANK Entry: 996.5 SL: 992.0556666666666 TP: 1014.881
                        details_match = re.search(r"(.*?) Entry: ([\d\.]+) SL: ([\d\.]+) TP: ([\d\.]+)", details_line)
                        if details_match:
                            signals.append({
                                'Gate': gate,
                                'Symbol': details_match.group(1).strip(),
                                'Entry': float(details_match.group(2)),
                                'SL': float(details_match.group(3)),
                                'TP': float(details_match.group(4)),
                                'Time': "N/A" # Timestamp is on previous line (Wait, it's not easily accessible here without more complex parsing, skipping for now)
                            })
                    except StopIteration:
                        break
            
            if "AUTO-EXECUTED" in line:
                # Line 1: AUTO-EXECUTED [Side]:
                side_match = re.search(r"AUTO-EXECUTED \[(.*?)\]:", line)
                if side_match:
                    side = side_match.group(1)
                    try:
                        line2 = next(iterator)
                        # Line 2: Symbol Qty: ... @ ... SL: ... TP:
                        # Example: NSE_EQ|HDFCBANK Qty: 225 @ 996.5 SL: ...
                        m2 = re.search(r"(.*?) Qty: (\d+) @ ([\d\.]+)", line2)
                        
                        line3 = next(iterator)
                        # Line 3: ... (Gate: Key)
                        m3 = re.search(r"\(Gate: (.*?)\)", line3)
                        
                        if m2:
                            executions.append({
                                'Side': side,
                                'Symbol': m2.group(1).strip(),
                                'Qty': int(m2.group(2)),
                                'EntryWait': float(m2.group(3)),
                                'GateKey': m3.group(1) if m3 else "Unknown"
                            })
                    except StopIteration:
                        break
            
            if "AUTO-EXIT" in line:
                # Line 1: AUTO-EXIT [Side]:
                side_match = re.search(r"AUTO-EXIT \[(.*?)\]:", line)
                if side_match:
                    try:
                        line2 = next(iterator)
                        # Line 2: Symbol @ Price Reason: ... PnL: ... (Gate
                        # Example: NSE_EQ|ICICIBANK @ 1366.4 Reason: TECH_SL_HIT PnL: -1678.5 (Gate
                        m2 = re.search(r"(.*?) @ ([\d\.]+) Reason: (.*?) PnL: ([\-\d\.]+)", line2)
                        
                        line3 = next(iterator)
                        # Line 3: Refreshed: Key)
                        m3 = re.search(r"Refreshed: (.*?)\)", line3)

                        if m2:
                            exits.append({
                                'Side': side_match.group(1),
                                'Symbol': m2.group(1).strip(),
                                'ExitPrice': float(m2.group(2)),
                                'Reason': m2.group(3).strip(),
                                'PnL': float(m2.group(4)),
                                'GateKey': m3.group(1) if m3 else "Unknown"
                            })
                    except StopIteration:
                        break

    return signals, executions, exits

def analyze_results(signals, executions, exits):
    print(f"Total Signals: {len(signals)}")
    print(f"Total Executions: {len(executions)}")
    print(f"Total Exits: {len(exits)}")

    # Convert to DF for easier analysis
    df_exits = pd.DataFrame(exits)
    
    if df_exits.empty:
        print("No trades completed.")
        return

    # Extract Gate from GateKey
    # Gates can have underscores (STUFF_S). Logic: match suffix.
    KNOWN_GATES = [
        "STUFF_S", "CRUSH_L", "REBID", "RESET",
        "HITCH_L", "HITCH_S", "CLOUD_L", "CLOUD_S",
        "RUBBER_L", "RUBBER_S", "SNAP_B", "SNAP_S",
        "VWAP_REC", "VWAP_REJ", "MAGNET",
        "ORB_L", "ORB_S", "LATE_SQ"
    ]
    
    def get_gate_name(key):
        if not key or key == 'null': return "Unknown"
        key = key.strip()
        # Try to match known gates at the end of the string
        for gate in KNOWN_GATES:
            if key.endswith("_" + gate):
                return gate
            if key == gate: # unlikely but possible
                return gate
        
        # Fallback: try split but maybe show full key if unsure
        return key.split('_')[-1] # Fallback to naive

    df_exits['Gate'] = df_exits['GateKey'].apply(get_gate_name)

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
    log_file = "backtest_java.log"
    print(f"Analyzing {log_file}...")
    s, e, out = parse_log_file(log_file)
    analyze_results(s, e, out)
