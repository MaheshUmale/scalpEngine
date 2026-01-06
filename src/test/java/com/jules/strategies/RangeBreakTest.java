package com.jules.strategies;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.Position;
import com.jules.trading.TradeSignal;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

public class RangeBreakTest {

    @Test
    public void testAnalyze_bullishBreakout_returnsLongSignal() {
        // 1. Setup: Create mock data for a bullish breakout
        List<Candle> candles = new ArrayList<>();
        // Establish a range between 95 and 105
        for (int i = 0; i < 19; i++) {
            candles.add(new Candle(i, 100, 105, 95, 100, 1000));
        }
        // A breakout candle with high volume
        candles.add(new Candle(19, 105, 107, 104, 106, 2500));

        Data data = new Data();
        data.setCandles(candles);

        // 2. Execution: Analyze the data
        RangeBreak strategy = new RangeBreak();
        TradeSignal signal = strategy.analyze(data);

        // 3. Assertion: Verify the signal is correct
        assertNotNull(signal, "A signal should be generated for a bullish breakout.");
        assertEquals(Position.LONG, signal.getPosition(), "The position should be LONG.");
        assertEquals(106, signal.getEntryPrice(), "The entry price is incorrect.");
        assertEquals(105, signal.getStopLoss(), "The stop loss should be at the top of the range.");
        assertEquals(116, signal.getTakeProfit(), "The take profit is not calculated correctly.");
    }

    @Test
    public void testAnalyze_noBreakout_returnsNull() {
        // 1. Setup: Create mock data where price stays within the range
        List<Candle> candles = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            candles.add(new Candle(i, 100, 105, 95, 100, 1000));
        }

        Data data = new Data();
        data.setCandles(candles);

        // 2. Execution: Analyze the data
        RangeBreak strategy = new RangeBreak();
        TradeSignal signal = strategy.analyze(data);

        // 3. Assertion: No signal should be generated
        assertNull(signal, "No signal should be generated when the price is within the range.");
    }
}
