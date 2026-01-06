package com.jules.policies;

import com.jules.Data;
import com.jules.trading.Candle;
import com.jules.trading.TradeSignal;
import com.jules.trading.Position;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

import java.util.ArrayList;
import java.util.List;

public class PolicyEngineTest {

    @Test
    public void testDecide_withRangeBreakoutSignal_calculatesPositionSize() {
        // 1. Setup: Create mock data to trigger a bullish Range Break
        List<Candle> candles = new ArrayList<>();
        // Create 19 candles to establish a range between 90 and 100
        for (int i = 0; i < 19; i++) {
            candles.add(new Candle(i, 95, 100, 90, 95, 1000));
        }
        // Add a breakout candle with high volume
        candles.add(new Candle(19, 100, 102, 99, 101, 2000));

        Data data = new Data();
        data.setCandles(candles);

        // 2. Execution: Run the policy engine
        PolicyEngine policyEngine = new PolicyEngine();
        TradeSignal signal = policyEngine.decide(data);

        // 3. Assertion: Verify the signal and position size
        assertNotNull(signal, "A trade signal should have been generated.");
        assertEquals(Position.LONG, signal.getPosition(), "The signal should be for a LONG position.");
        assertEquals(101, signal.getEntryPrice(), "The entry price is incorrect.");
        assertEquals(100, signal.getStopLoss(), "The stop loss is incorrect.");

        // Expected position size calculation:
        // accountSize = 100000, maxRiskPerTrade = 0.01 -> riskAmount = 1000
        // riskPerShare = entryPrice - stopLoss = 101 - 100 = 1
        // positionSize = riskAmount / riskPerShare = 1000 / 1 = 1000
        assertEquals(1000, signal.getPositionSize(), "The position size calculation is incorrect.");
    }
}
