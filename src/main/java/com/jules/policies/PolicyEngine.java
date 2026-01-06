package com.jules.policies;

import com.jules.Data;
import com.jules.trading.TradeSignal;
import com.jules.trading.TradingStrategy;
import com.jules.strategies.RangeBreak;
import com.jules.strategies.SecondChanceScalp;
import com.jules.strategies.FailedBreakout;

import java.util.ArrayList;
import java.util.List;

public class PolicyEngine {
    private final List<TradingStrategy> strategies = new ArrayList<>();
    private final double accountSize = 100000; // Example account size of $100,000
    private final double maxRiskPerTrade = 0.01; // Risk 1% of account per trade

    public PolicyEngine() {
        // Instantiate all available strategies
        strategies.add(new RangeBreak());
        strategies.add(new SecondChanceScalp());
        strategies.add(new FailedBreakout());
    }

    public TradeSignal decide(Data data) {
        for (TradingStrategy strategy : strategies) {
            TradeSignal signal = strategy.analyze(data);
            if (signal != null) {
                // Set the strategy name
                signal.setStrategyName(strategy.getName());

                // Apply risk management and position sizing
                double riskAmount = accountSize * maxRiskPerTrade;
                double riskPerShare = Math.abs(signal.getEntryPrice() - signal.getStopLoss());

                if (riskPerShare > 0) {
                    double positionSize = riskAmount / riskPerShare;
                    signal.setPositionSize(positionSize);
                    return signal;
                }
            }
        }
        return null; // No signal found
    }
}
