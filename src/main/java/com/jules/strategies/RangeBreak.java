package com.jules.strategies;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.Position;
import com.jules.trading.TradeSignal;
import com.jules.trading.TradingStrategy;

import java.util.List;
import java.util.stream.Collectors;

public class RangeBreak implements TradingStrategy {

    @Override
    public String getName() {
        return "Range Break";
    }

    @Override
    public TradeSignal analyze(Data data) {
        List<Candle> candles = data.getCandles();
        if (candles.size() < 20) {
            return null; // Not enough data
        }

        // 1. Define the range using the last 20 candles
        List<Candle> rangeCandles = candles.subList(candles.size() - 20, candles.size() - 1);
        double high = rangeCandles.stream().mapToDouble(Candle::getHigh).max().orElse(Double.MAX_VALUE);
        double low = rangeCandles.stream().mapToDouble(Candle::getLow).min().orElse(Double.MIN_VALUE);

        // 2. Calculate average volume in the range
        double avgVolume = rangeCandles.stream().mapToDouble(Candle::getVolume).average().orElse(0);

        Candle currentCandle = candles.get(candles.size() - 1);

        // 3. Check for a breakout with a volume surge (1.5x average)
        if (currentCandle.getClose() > high && currentCandle.getVolume() > avgVolume * 1.5) {
            // Bullish breakout
            double entryPrice = currentCandle.getClose();
            double stopLoss = high; // Place stop loss at the previous resistance
            double takeProfit = entryPrice + (high - low); // Target a move equal to the range size
            return new TradeSignal(Position.LONG, entryPrice, stopLoss, takeProfit);
        } else if (currentCandle.getClose() < low && currentCandle.getVolume() > avgVolume * 1.5) {
            // Bearish breakout
            double entryPrice = currentCandle.getClose();
            double stopLoss = low; // Place stop loss at the previous support
            double takeProfit = entryPrice - (high - low); // Target a move equal to the range size
            return new TradeSignal(Position.SHORT, entryPrice, stopLoss, takeProfit);
        }

        return null;
    }
}
