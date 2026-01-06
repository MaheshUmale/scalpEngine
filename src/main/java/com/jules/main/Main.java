package com.jules.main;

import com.jules.Data;
import com.jules.client.TradingWebSocketClient;
import com.jules.policies.PolicyEngine;
import com.jules.trading.Candle;
import com.jules.trading.TradeSignal;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Main {

    private static final Map<String, List<Candle>> candleHistory = new HashMap<>();
    private static final int MAX_HISTORY_SIZE = 50; // Keep last 50 candles
    private static PolicyEngine policyEngine;

    public static void main(String[] args) {
        try {
            // 1. Initialize the Policy Engine
            PolicyEngine policyEngine = new PolicyEngine();
            final List<Candle> historicalCandles = new ArrayList<>();

            // 2. Connect to the WebSocket Server
            TradingWebSocketClient client = new TradingWebSocketClient(new URI("ws://localhost:8765"));

            // 3. Set up a message handler to process incoming data
            client.addMessageHandler(message -> {
                try {
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(message);
                    String type = (String) json.get("type");

                    if ("candle_update".equals(type)) { // FIX: Listen for the correct message type
                        JSONArray dataArray = (JSONArray) json.get("data");
                        if (dataArray == null) return;

                        // Process the latest candle data from the message
                        for (Object obj : dataArray) {
                            JSONObject candleUpdate = (JSONObject) obj;
                            JSONObject candleJson = (JSONObject) candleUpdate.get("1m"); // Use 1-minute candle

                            Candle newCandle = new Candle(
                                    (long) candleUpdate.get("timestamp"),
                                    ((Number) candleJson.get("open")).doubleValue(),
                                    ((Number) candleJson.get("high")).doubleValue(),
                                    ((Number) candleJson.get("low")).doubleValue(),
                                    ((Number) candleJson.get("close")).doubleValue(),
                                    ((Number) candleJson.get("volume")).intValue()
                            );

                            // Add to historical data, ensuring no duplicates and maintaining order
                            if (historicalCandles.isEmpty() || newCandle.getTimestamp() > historicalCandles.get(historicalCandles.size() - 1).getTimestamp()) {
                                historicalCandles.add(newCandle);
                            }
                        }

                        // Keep the list of candles to a reasonable size for performance
                        if (historicalCandles.size() > 100) {
                            historicalCandles.remove(0);
                        }

                        // Create a Data object and run the policy engine
                        Data data = new Data();
                        data.setCandles(new ArrayList<>(historicalCandles)); // Pass a copy
                        TradeSignal signal = policyEngine.decide(data);

                        // If a signal is generated, print it
                        if (signal != null) {
                            System.out.println("New Trade Signal: " + signal.getPosition() +
                                               " | Entry: " + signal.getEntryPrice() +
                                               " | Stop: " + signal.getStopLoss() +
                                               " | Take Profit: " + signal.getTakeProfit() +
                                               " | Position Size: " + String.format("%.2f", signal.getPositionSize()));
                        }
                    }
                } catch (ParseException e) {
                    System.err.println("Error parsing message: " + message);
                    e.printStackTrace();
                }
            });

            // 4. Start the client
            System.out.println("Trading engine started. Connecting to data bridge...");
            client.connectBlocking();

        } catch (URISyntaxException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    private static void processCandleUpdate(JSONObject updateJson) {
        String symbol = (String) updateJson.get("symbol");
        JSONObject candleJson = (JSONObject) updateJson.get("1m");

        // Create a candle from the received data
        Candle newCandle = new Candle(
            (long) updateJson.get("timestamp"),
            (double) candleJson.get("open"),
            (double) candleJson.get("high"),
            (double) candleJson.get("low"),
            (double) candleJson.get("close"),
            ((Long) candleJson.get("volume")).intValue()
        );

        // Get or create the history for this symbol
        List<Candle> symbolHistory = candleHistory.computeIfAbsent(symbol, k -> new ArrayList<>());
        symbolHistory.add(newCandle);

        // Maintain history size
        if (symbolHistory.size() > MAX_HISTORY_SIZE) {
            symbolHistory.remove(0);
        }

        // Log the size of the history
        System.out.println("Processing " + symbol + " with " + symbolHistory.size() + " candles in history.");
        if (!symbolHistory.isEmpty()) {
            System.out.println("Latest candle: " + symbolHistory.get(symbolHistory.size() - 1));
        }

        // Create a Data object with the historical data for the current symbol and run the policy engine
        Data data = new Data();
        data.setCandles(symbolHistory);
        TradeSignal signal = policyEngine.decide(data);

        // If a signal is generated, print it
        if (signal != null) {
            signal.setSymbol(symbol);
            // Add symbol and strategy name to the output for clarity
            System.out.println("SCALP SIGNAL [" + signal.getStrategyName() + "] for " + signal.getSymbol() + ": " + signal.getPosition() +
                               " | Entry: " + signal.getEntryPrice() +
                               " | Stop: " + signal.getStopLoss() +
                               " | Take Profit: " + signal.getTakeProfit() +
                               " | Position Size: " + signal.getPositionSize());
        }
    }
}
