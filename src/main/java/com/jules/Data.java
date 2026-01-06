package com.jules;

import com.jules.trading.Candle;
import java.util.List;

public class Data {
    private List<Candle> candles;
    // Add other data fields as needed, e.g., order book, option chains

    public List<Candle> getCandles() {
        return candles;
    }

    public void setCandles(List<Candle> candles) {
        this.candles = candles;
    }
}
