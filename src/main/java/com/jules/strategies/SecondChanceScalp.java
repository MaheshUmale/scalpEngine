package com.jules.strategies;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.Position;
import com.jules.trading.TradeSignal;
import com.jules.trading.TradingStrategy;

import java.util.List;

public class SecondChanceScalp implements TradingStrategy {

    @Override
    public String getName() {
        return "Second Chance Scalp";
    }

    @Override
    public TradeSignal analyze(Data data) {
        List<Candle> candles = data.getCandles();
        // Require at least 23 candles to identify a 20-candle range plus the 3-candle pattern
        if (candles.size() < 23) {
            return null;
        }

        // The pattern consists of: Range -> Breakout -> Retest -> Confirmation
        Candle confirmationCandle = candles.get(candles.size() - 1);
        Candle retestCandle = candles.get(candles.size() - 2);
        Candle breakoutCandle = candles.get(candles.size() - 3);

        // Define the range from the 20 candles prior to the breakout
        List<Candle> rangeCandles = candles.subList(candles.size() - 23, candles.size() - 3);
        double resistance = rangeCandles.stream().mapToDouble(Candle::getHigh).max().orElse(Double.MAX_VALUE);

        // 1. Check for a clear breakout
        boolean isBreakout = breakoutCandle.getClose() > resistance;
        if (!isBreakout) {
            return null;
        }

        // 2. Check for a retest of the resistance level
        boolean isRetest = retestCandle.getLow() <= resistance && retestCandle.getClose() > resistance;
        if (!isRetest) {
            return null;
        }

        // 3. Check for a bullish confirmation candle
        boolean isConfirmed = confirmationCandle.getClose() > confirmationCandle.getOpen();
        if (isConfirmed) {
            // Valid signal found
            return new TradeSignal(
                Position.LONG,
                confirmationCandle.getClose(),
                resistance, // Stop loss at the old resistance level
                confirmationCandle.getClose() + (confirmationCandle.getClose() - resistance) * 2 // Example take profit (2:1 reward/risk)
            );
        }

        return null;
    }
}
