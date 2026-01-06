package com.jules.strategies;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.Position;
import com.jules.trading.TradeSignal;
import com.jules.trading.TradingStrategy;

import java.util.List;

public class FailedBreakout implements TradingStrategy {

    @Override
    public String getName() {
        return "Failed Breakout";
    }

    @Override
    public TradeSignal analyze(Data data) {
        List<Candle> candles = data.getCandles();
        if (candles.size() < 20) {
            return null; // Not enough data to establish a range
        }

        // 1. Establish a trading range from the last 20 candles (excluding the most recent ones)
        List<Candle> rangeCandles = candles.subList(candles.size() - 20, candles.size() - 2);
        double resistance = rangeCandles.stream().mapToDouble(Candle::getHigh).max().orElse(Double.MAX_VALUE);
        double support = rangeCandles.stream().mapToDouble(Candle::getLow).min().orElse(Double.MIN_VALUE);

        Candle currentCandle = candles.get(candles.size() - 1);
        Candle previousCandle = candles.get(candles.size() - 2);

        // 2. Detect a breakout above resistance
        boolean breakoutOccurred = previousCandle.getClose() > resistance;

        // 3. Detect a failure: the next candle closes back inside the range
        if (breakoutOccurred && currentCandle.getClose() < resistance) {
            // This is a bearish failed breakout pattern
            double entryPrice = currentCandle.getClose();
            double stopLoss = previousCandle.getHigh(); // Place stop above the high of the failed breakout candle
            double takeProfit = support; // Target the bottom of the range

            return new TradeSignal(Position.SHORT, entryPrice, stopLoss, takeProfit);
        }

        return null;
    }
}
