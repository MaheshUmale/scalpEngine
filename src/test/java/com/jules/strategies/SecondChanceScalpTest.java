package com.jules.strategies;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.Position;
import com.jules.trading.TradeSignal;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

public class SecondChanceScalpTest {

    @Test
    public void testAnalyze_validPattern_returnsLongSignal() {
        // 1. Setup: Create a specific candle sequence for the pattern
        List<Candle> candles = new ArrayList<>();

        // a. Establish a range with resistance at 100
        for (int i = 0; i < 20; i++) {
            candles.add(new Candle(i, 98, 100, 96, 98, 1000));
        }

        // b. Breakout candle
        candles.add(new Candle(20, 100, 102, 99, 101, 1500));

        // c. Retest candle (dips to 100, closes above)
        candles.add(new Candle(21, 101, 101.5, 100, 101, 1200));

        // d. Bullish confirmation candle
        candles.add(new Candle(22, 101, 103, 100.5, 102, 1800));

        Data data = new Data();
        data.setCandles(candles);

        // 2. Execution
        SecondChanceScalp strategy = new SecondChanceScalp();
        TradeSignal signal = strategy.analyze(data);

        // 3. Assertion
        assertNotNull(signal, "A signal should be generated for a valid second chance scalp pattern.");
        assertEquals(Position.LONG, signal.getPosition(), "The position should be LONG.");
        assertEquals(102, signal.getEntryPrice(), "The entry price should be the close of the last candle.");
        assertEquals(100.0, signal.getStopLoss(), "The stop loss should be at the resistance level.");
    }

    @Test
    public void testAnalyze_invalidPattern_returnsNull() {
        // 1. Setup: Create a sequence that fails the retest
        List<Candle> candles = new ArrayList<>();

        // a. Range with resistance at 100
        for (int i = 0; i < 20; i++) {
            candles.add(new Candle(i, 98, 100, 96, 98, 1000));
        }

        // b. Breakout candle
        candles.add(new Candle(20, 100, 102, 99, 101, 1500));

        // c. Failed retest (closes below resistance)
        candles.add(new Candle(21, 101, 101.5, 99, 99.5, 1200));

        // d. Another candle
        candles.add(new Candle(22, 99.5, 100, 98, 99, 1800));

        Data data = new Data();
        data.setCandles(candles);

        // 2. Execution
        SecondChanceScalp strategy = new SecondChanceScalp();
        TradeSignal signal = strategy.analyze(data);

        // 3. Assertion
        assertNull(signal, "No signal should be generated if the retest fails.");
    }
}
