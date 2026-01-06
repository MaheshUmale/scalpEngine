package com.jules.trading;

public class TradeSignal {
    private final Position position;
    private final double entryPrice;
    private final double stopLoss;
    private final double takeProfit;
    private double positionSize;
    private String strategyName;
    private String symbol;

    public TradeSignal(Position position, double entryPrice, double stopLoss, double takeProfit) {
        this.position = position;
        this.entryPrice = entryPrice;
        this.stopLoss = stopLoss;
        this.takeProfit = takeProfit;
    }

    public Position getPosition() {
        return position;
    }

    public double getEntryPrice() {
        return entryPrice;
    }

    public double getStopLoss() {
        return stopLoss;
    }

    public double getTakeProfit() {
        return takeProfit;
    }

    public double getPositionSize() {
        return positionSize;
    }

    public void setPositionSize(double positionSize) {
        this.positionSize = positionSize;
    }

    public String getStrategyName() {
        return strategyName;
    }

    public void setStrategyName(String strategyName) {
        this.strategyName = strategyName;
    }

    public String getSymbol() {
        return symbol;
    }

    public void setSymbol(String symbol) {
        this.symbol = symbol;
    }
}
