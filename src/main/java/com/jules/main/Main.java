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

public class Main {

    public static void main(String[] args) {
        try {
            // 1. Initialize the Policy Engine
            PolicyEngine policyEngine = new PolicyEngine();

            // 2. Connect to the WebSocket Server
            TradingWebSocketClient client = new TradingWebSocketClient(new URI("ws://localhost:8765"));

            // 3. Set up a message handler to process incoming data
            client.addMessageHandler(message -> {
                try {
                    // Parse the incoming JSON message
                    JSONParser parser = new JSONParser();
                    JSONObject json = (JSONObject) parser.parse(message);
                    String type = (String) json.get("type");

                    if ("market_data".equals(type)) {
                        // Assuming the data is a list of candles
                        JSONArray candleArray = (JSONArray) json.get("candles");
                        List<Candle> candles = new ArrayList<>();
                        for (Object obj : candleArray) {
                            JSONObject candleJson = (JSONObject) obj;
                            candles.add(new Candle(
                                (long) candleJson.get("timestamp"),
                                (double) candleJson.get("open"),
                                (double) candleJson.get("high"),
                                (double) candleJson.get("low"),
                                (double) candleJson.get("close"),
                                ((Long) candleJson.get("volume")).intValue()
                            ));
                        }

                        // Create a Data object and run the policy engine
                        Data data = new Data();
                        data.setCandles(candles);
                        TradeSignal signal = policyEngine.decide(data);

                        // If a signal is generated, print it
                        if (signal != null) {
                            System.out.println("New Trade Signal: " + signal.getPosition() +
                                               " | Entry: " + signal.getEntryPrice() +
                                               " | Stop: " + signal.getStopLoss() +
                                               " | Take Profit: " + signal.getTakeProfit() +
                                               " | Position Size: " + signal.getPositionSize());
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
}
