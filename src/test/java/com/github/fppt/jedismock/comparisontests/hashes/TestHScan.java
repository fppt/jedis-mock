package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static redis.clients.jedis.params.ScanParams.SCAN_POINTER_START;

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
            assertThat(mapResult.put(entry.getKey(), entry.getValue())).isNull();
        }
        assertThat(mapResult).isEqualTo(expected);
    }

    @TestTemplate
    public void hscanReturnsPartialSet(Jedis jedis) {
        for (int i = 0; i < 1024; i++) {
            jedis.hset(key, "hkey" + i, "hval" + i);
        }
        ScanResult<Map.Entry<String, String>> result = jedis.hscan(key,
                ScanParams.SCAN_POINTER_START,
                new ScanParams().count(7));
        assertThat(result.getCursor()).isNotEqualTo(SCAN_POINTER_START);
    }

    @TestTemplate
    public void hscanReturnsMatchingSet(Jedis jedis) {
        for (int i = 0; i < 9; i++) {
            jedis.hset(key, "hkey" + i, "hval" + i);
        }

        ScanResult<Map.Entry<String, String>> result = jedis.hscan(key,
                ScanParams.SCAN_POINTER_START,
                new ScanParams().match("hkey7"));

        assertThat(result.getCursor()).isEqualTo(SCAN_POINTER_START);
        assertThat(result.getResult()).hasSize(1);
        assertThat(result.getResult().get(0).getKey()).isEqualTo("hkey7");
        assertThat(result.getResult().get(0).getValue()).isEqualTo("hval7");
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
                assertThat(results.put(entry.getKey(), entry.getValue())).isNull();
            }
            count++;
        } while (!ScanParams.SCAN_POINTER_START.equals(cursor));
        assertThat(results).containsAllEntriesOf(expected);
        assertThat(count).isGreaterThan(1);
    }
}
