package com.jules.backtest;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.Position;
import com.jules.trading.TradeSignal;
import com.jules.trading.TradingStrategy;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

class Trade {
    public double entryPrice;
    public double exitPrice;
    public Position position;
    public double stopLoss;
    public double takeProfit;
    public boolean open = true;

    public Trade(TradeSignal signal) {
        this.entryPrice = signal.getEntryPrice();
        this.position = signal.getPosition();
        this.stopLoss = signal.getStopLoss();
        this.takeProfit = signal.getTakeProfit();
    }
}

public class Backtester {
    private final TradingStrategy strategy;
    private final List<Candle> historicalData;
    private final List<Trade> trades = new ArrayList<>();
    private Trade currentTrade = null;

    public Backtester(TradingStrategy strategy, String csvFilePath) {
        this.strategy = strategy;
        this.historicalData = loadDataFromCsv(csvFilePath);
    }

    private List<Candle> loadDataFromCsv(String filePath) {
        List<Candle> candles = new ArrayList<>();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            br.readLine(); // Skip header
            while ((line = br.readLine()) != null) {
                if (line.trim().startsWith("#") || line.trim().isEmpty()) {
                    continue; // Skip comment lines and empty lines
                }

                String cleanLine = line.split("#")[0].trim();
                String[] values = cleanLine.split(",");

                long timestamp = LocalDateTime.parse(values[0].trim(), formatter).toEpochSecond(ZoneOffset.UTC);
                candles.add(new Candle(
                    timestamp,
                    Double.parseDouble(values[1].trim()), // open
                    Double.parseDouble(values[2].trim()), // high
                    Double.parseDouble(values[3].trim()), // low
                    Double.parseDouble(values[4].trim()), // close
                    Integer.parseInt(values[5].trim())    // volume
                ));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return candles;
    }

    public void run() {
        System.out.println("Running backtest for strategy: " + strategy.getName());

        if (historicalData.size() < 23) {
            System.out.println("Not enough historical data to run backtest. Need at least 23 candles.");
            return;
        }

        for (int i = 23; i < historicalData.size(); i++) {
            List<Candle> currentCandles = new ArrayList<>(historicalData.subList(0, i + 1));

            if (currentTrade != null && currentTrade.open) {
                Candle currentCandle = currentCandles.get(currentCandles.size() - 1);
                if (currentTrade.position == Position.LONG) {
                    if (currentCandle.getLow() <= currentTrade.stopLoss) {
                        currentTrade.exitPrice = currentTrade.stopLoss;
                        currentTrade.open = false;
                    } else if (currentCandle.getHigh() >= currentTrade.takeProfit) {
                        currentTrade.exitPrice = currentTrade.takeProfit;
                        currentTrade.open = false;
                    }
                } else { // SHORT
                    if (currentCandle.getHigh() >= currentTrade.stopLoss) {
                        currentTrade.exitPrice = currentTrade.stopLoss;
                        currentTrade.open = false;
                    } else if (currentCandle.getLow() <= currentTrade.takeProfit) {
                        currentTrade.exitPrice = currentTrade.takeProfit;
                        currentTrade.open = false;
                    }
                }
                if (!currentTrade.open) {
                    trades.add(currentTrade);
                    currentTrade = null;
                }
            }

            if (currentTrade == null) {
                Data data = new Data();
                data.setCandles(currentCandles);
                TradeSignal signal = strategy.analyze(data);

                if (signal != null) {
                    currentTrade = new Trade(signal);
                }
            }
        }

        // Final check: if a trade is still open, close it at the last candle's close
        if (currentTrade != null && currentTrade.open) {
            currentTrade.exitPrice = historicalData.get(historicalData.size() - 1).getClose();
            currentTrade.open = false;
            trades.add(currentTrade);
        }

        PerformanceReport report = new PerformanceReport(strategy.getName(), trades);
        report.printReport();

        System.out.println("Backtest complete.");
    }
}
