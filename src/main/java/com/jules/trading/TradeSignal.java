package com.jules.trading;

public class TradeSignal {
    private final Position position;
    private final double entryPrice;
    private final double stopLoss;
    private final double takeProfit;
    private double positionSize;

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
}
