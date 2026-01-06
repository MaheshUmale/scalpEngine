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
import java.util.List;
import java.util.stream.Collectors;

public class Main {

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
}
