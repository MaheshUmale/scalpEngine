package com.jules.backtest;

import com.jules.trading.Position;
import java.util.List;
import java.util.stream.Collectors;

public class PerformanceReport {
    private final List<Trade> trades;
    private final String strategyName;

    public PerformanceReport(String strategyName, List<Trade> trades) {
        this.strategyName = strategyName;
        this.trades = trades;
    }

    public void printReport() {
        System.out.println("\n--- Backtest Performance Report ---");
        System.out.println("Strategy: " + strategyName);
        System.out.println("------------------------------------");

        if (trades.isEmpty()) {
            System.out.println("No trades were executed.");
            return;
        }

        double totalPnl = calculateTotalPnl();
        double grossProfit = calculateGrossProfit();
        double grossLoss = calculateGrossLoss();
        double profitFactor = grossLoss != 0 ? grossProfit / Math.abs(grossLoss) : Double.POSITIVE_INFINITY;
        long numTrades = trades.size();
        long numWinners = countWinningTrades();
        long numLosers = numTrades - numWinners;
        double winRate = numTrades > 0 ? (double) numWinners / numTrades * 100 : 0;
        double avgWin = numWinners > 0 ? grossProfit / numWinners : 0;
        double avgLoss = numLosers > 0 ? grossLoss / numLosers : 0;

        System.out.printf("Total Trades: %d%n", numTrades);
        System.out.printf("Net PnL: %.2f%n", totalPnl);
        System.out.printf("Profit Factor: %.2f%n", profitFactor);
        System.out.printf("Win Rate: %.2f%%%n", winRate);
        System.out.printf("Average Win: %.2f%n", avgWin);
        System.out.printf("Average Loss: %.2f%n", avgLoss);
        System.out.println("------------------------------------");
    }

    private double calculateTotalPnl() {
        return trades.stream().mapToDouble(this::calculatePnlForTrade).sum();
    }

    private double calculateGrossProfit() {
        return trades.stream()
            .mapToDouble(this::calculatePnlForTrade)
            .filter(pnl -> pnl > 0)
            .sum();
    }

    private double calculateGrossLoss() {
        return trades.stream()
            .mapToDouble(this::calculatePnlForTrade)
            .filter(pnl -> pnl < 0)
            .sum();
    }

    private long countWinningTrades() {
        return trades.stream().filter(trade -> calculatePnlForTrade(trade) > 0).count();
    }

    private double calculatePnlForTrade(Trade trade) {
        if (trade.position == Position.LONG) {
            return trade.exitPrice - trade.entryPrice;
        } else {
            return trade.entryPrice - trade.exitPrice;
        }
    }

    // Getter to be used by the Backtester
    public List<Trade> getTrades() {
        return trades;
    }
}
