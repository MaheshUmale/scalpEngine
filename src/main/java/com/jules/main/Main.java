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

public class Main {

    private static final Map<String, List<Candle>> candleHistory = new HashMap<>();
    private static final int MAX_HISTORY_SIZE = 50; // Keep last 50 candles
    private static PolicyEngine policyEngine;

    public static void main(String[] args) {
        try {
            // 1. Initialize the Policy Engine
            policyEngine = new PolicyEngine();

            // 2. Connect to the WebSocket Server
            TradingWebSocketClient client = new TradingWebSocketClient(new URI("ws://localhost:8765"));

            // 3. Set up a message handler to process incoming data
            client.addMessageHandler(message -> {
                try {
                    // Parse the incoming JSON message
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(message);
                    String type = (String) json.get("type");

                    if ("candle_update".equals(type)) {
                        // Data is a list of candle updates for multiple symbols
                        JSONArray candleUpdates = (JSONArray) json.get("data");

                        for (Object obj : candleUpdates) {
                            processCandleUpdate((JSONObject) obj);
                        }
                    }
                } catch (ParseException e) {
                    System.err.println("Error parsing message: " + message);
                    e.printStackTrace();
                }
            });

            // 4. Start the client
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
