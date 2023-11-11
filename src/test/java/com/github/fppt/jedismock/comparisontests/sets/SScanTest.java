package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class SScanTest {
    private static final String key = "sscankey";

    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void sscanReturnsAllValues(Jedis jedis) {
        String[] values = new String[20];
        for (int i = 0; i < 20; i++) {
            values[i] = (21 - i) + "_value_" + i;
        }
        jedis.sadd(key, values);

        ScanResult<String> result = jedis.sscan(key, ScanParams.SCAN_POINTER_START, new ScanParams().count(30));

        assertEquals(20, result.getResult().size());
        assertTrue(result.getResult().contains(values[1]));
    }

    @TestTemplate
    public void sscanReturnsPartialSet(Jedis jedis) {
        final int elementsNumber = 1120;
        String[] values = new String[elementsNumber];
        for (int i = 0; i < elementsNumber; i++) {
            values[i] = (elementsNumber + 1 - i) + "_value_" + i;
        }
        jedis.sadd(key, values);

        ScanResult<String> result = jedis.sscan(key, ScanParams.SCAN_POINTER_START, new ScanParams().count(13));
        assertNotEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
    }

    @TestTemplate
    public void sscanReturnsMatchingSet(Jedis jedis) {
        String[] values = new String[9];
        for (int i = 0; i < 9; i++) {
            values[i] = (21 - i) + "_value_" + i;
        }
        jedis.sadd(key, values);

        ScanResult<String> result = jedis.sscan(key, ScanParams.SCAN_POINTER_START,
                new ScanParams().match("21_value_0"));

        assertEquals(ScanParams.SCAN_POINTER_START, result.getCursor());
        assertEquals(1, result.getResult().size());
        assertTrue(result.getResult().contains(values[0]));
    }

    @TestTemplate
    public void sscanIterates(Jedis jedis) {
        final int numOfElements = 1145;

        String[] values = new String[numOfElements];
        for (int i = 0; i < numOfElements; i++) {
            values[i] = (numOfElements - i) + "_value_" + i;
        }
        jedis.sadd(key, values);

        Set<String> results = new HashSet<>();
        String cursor = ScanParams.SCAN_POINTER_START;
        int count = 0;
        do {
            ScanResult<String> result = jedis.sscan(key, cursor);
            cursor = result.getCursor();
            results.addAll(result.getResult());
            count++;
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        assertEquals(new HashSet<>(Arrays.asList(values)), results);
        assertTrue(count > 1);

    }
}
