package com.jules.trading;

import com.jules.Data;

public interface TradingStrategy {
    String getName();
    TradeSignal analyze(Data data);
}
