package com.jules.backtest;

import com.jules.strategies.FailedBreakout;
import com.jules.strategies.RangeBreak;
import com.jules.strategies.SecondChanceScalp;
import com.jules.trading.TradingStrategy;

import java.util.ArrayList;
import java.util.List;

public class BacktestRunner {

    public static void main(String[] args) {
        String csvFilePath = "nifty_1min_data.csv";

        // List of strategies to backtest
        List<TradingStrategy> strategies = new ArrayList<>();
        strategies.add(new RangeBreak());
        strategies.add(new SecondChanceScalp());
        strategies.add(new FailedBreakout());

        // Run the backtester for each strategy
        for (TradingStrategy strategy : strategies) {
            Backtester backtester = new Backtester(strategy, csvFilePath);
            backtester.run();
        }
    }
}
