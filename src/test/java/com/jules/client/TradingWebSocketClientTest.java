package com.jules.client;

import org.java_websocket.handshake.ClientHandshake;
import org.java_websocket.server.WebSocketServer;
import org.java_websocket.WebSocket;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class TradingWebSocketClientTest {

    private MockWebSocketServer mockServer;
    private TradingWebSocketClient client;
    private final BlockingQueue<String> receivedMessages = new LinkedBlockingQueue<>();
    private final int port = 8765;

    // A simple mock server for testing
    private static class MockWebSocketServer extends WebSocketServer {
        public MockWebSocketServer(int port) {
            super(new InetSocketAddress(port));
        }

        @Override
        public void onOpen(WebSocket conn, ClientHandshake handshake) {
            System.out.println("MockServer: New connection from " + conn.getRemoteSocketAddress());
        }

        @Override
        public void onClose(WebSocket conn, int code, String reason, boolean remote) {
            System.out.println("MockServer: Closed connection to " + conn.getRemoteSocketAddress() + " with code " + code + " and reason " + reason);
        }

        @Override
        public void onMessage(WebSocket conn, String message) {
            // Not used in this test
        }

        @Override
        public void onError(WebSocket conn, Exception ex) {
            ex.printStackTrace();
        }

        @Override
        public void onStart() {
            System.out.println("MockServer: Started successfully on port " + getPort());
        }

        public void broadcastMessage(String message) {
            broadcast(message);
        }
    }

    @BeforeEach
    public void setUp() throws Exception {
        // Kill any process that might be lingering on the port
        try {
            Process p = Runtime.getRuntime().exec("kill $(lsof -t -i:" + port + ") 2>/dev/null || true");
            p.waitFor();
        } catch (IOException | InterruptedException e) {
            // Ignore errors, as the process may not exist
        }

        // Start the mock server
        mockServer = new MockWebSocketServer(port);
        mockServer.start();

        // Add a small delay to ensure the server is fully started
        try {
            Thread.sleep(2000); // 2 second delay
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Initialize and connect the client
        client = new TradingWebSocketClient(new URI("ws://localhost:" + port));
        client.addMessageHandler(receivedMessages::add);
        client.connectBlocking();
    }

    @AfterEach
    public void tearDown() throws Exception {
        // Stop the client and server
        if (client != null) {
            client.closeBlocking();
        }
        if (mockServer != null) {
            mockServer.stop(1000); // Add a timeout to ensure clean shutdown
        }
    }

    @Test
    public void testOnMessage_receivesMessageFromServer() throws InterruptedException {
        // 1. Setup: Define a test message
        String testMessage = "{\"type\":\"test\",\"data\":\"hello\"}";

        // 2. Execution: Server broadcasts a message
        mockServer.broadcastMessage(testMessage);

        // 3. Assertion: Verify the client received the message
        String received = receivedMessages.poll(2, TimeUnit.SECONDS);
        assertNotNull(received, "Client should have received a message from the server.");
        assertEquals(testMessage, received, "The received message does not match the sent message.");
    }
}
