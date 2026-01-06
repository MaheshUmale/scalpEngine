package com.jules.trading;

public class Candle {
    private final long timestamp;
    private final double open;
    private final double high;
    private final double low;
    private final double close;
    private final int volume;

    public Candle(long timestamp, double open, double high, double low, double close, int volume) {
        this.timestamp = timestamp;
        this.open = open;
        this.high = high;
        this.low = low;
        this.close = close;
        this.volume = volume;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public double getOpen() {
        return open;
    }

    public double getHigh() {
        return high;
    }

    public double getLow() {
        return low;
    }

    public double getClose() {
        return close;
    }

    public int getVolume() {
        return volume;
    }
}
