package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.ScanParams;
import redis.clients.jedis.ScanResult;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestHScan {

    private static final String key = "hscankey";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }


    @TestTemplate
    public void hscanReturnsAllValues(Jedis jedis) {
        Map<String, String> expected = new HashMap<>();
        for (int i = 0; i < 20; i++) {
            jedis.hset(key, "hkey" + i, "hval" + i);
            expected.put("hkey" + i, "hval" + i);
        }
        ScanResult<Map.Entry<String, String>> result =
                jedis.hscan(key, ScanParams.SCAN_POINTER_START, new ScanParams().count(30));

        Map<String, String> mapResult = new HashMap<>();
        for (Map.Entry<String, String> entry : result.getResult()) {
            assertNull(mapResult.put(entry.getKey(), entry.getValue()));
        }
        assertEquals(expected, mapResult);
    }

    @TestTemplate
    public void hscanReturnsPartialSet(Jedis jedis) {
        for (int i = 0; i < 1024; i++) {
            jedis.hset(key, "hkey" + i, "hval" + i);
        }
        ScanResult<Map.Entry<String, String>> result = jedis.hscan(key,
                ScanParams.SCAN_POINTER_START,
                new ScanParams().count(7));
        assertNotEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
    }

    @TestTemplate
    public void hscanReturnsMatchingSet(Jedis jedis) {
        for (int i = 0; i < 9; i++) {
            jedis.hset(key, "hkey" + i, "hval" + i);
        }

        ScanResult<Map.Entry<String, String>> result = jedis.hscan(key,
                ScanParams.SCAN_POINTER_START,
                new ScanParams().match("hkey7"));

        assertEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
        assertEquals(1, result.getResult().size());
        assertEquals("hkey7", result.getResult().get(0).getKey());
        assertEquals("hval7", result.getResult().get(0).getValue());
    }

    @TestTemplate
    public void hscanIterates(Jedis jedis) {
        Map<String, String> expected = new HashMap<>();
        for (int i = 0; i < 1024; i++) {
            jedis.hset(key, "hkey" + i, "hval" + i);
            expected.put("hkey" + i, "hval" + i);
        }
        String cursor = ScanParams.SCAN_POINTER_START;
        Map<String, String> results = new HashMap<>();
        int count = 0;
        do {
            ScanResult<Map.Entry<String, String>> result = jedis.hscan(key, cursor);
            cursor = result.getCursor();
            for (Map.Entry<String, String> entry : result.getResult()) {
                assertNull(results.put(entry.getKey(), entry.getValue()));
            }
            count++;
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        assertEquals(expected, results);
        assertTrue(count > 1);
    }
}
