package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ScanParams;
import redis.clients.jedis.resps.ScanResult;
import redis.clients.jedis.resps.Tuple;

import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZScan {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        for (int i = 0; i < 10; i++) {
            jedis.zadd(ZSET_KEY, i, "a" + i);
        }
    }

    @TestTemplate
    public void testZScanWithPattern(Jedis jedis) {
        ScanResult<Tuple> result = jedis.zscan(ZSET_KEY, "0", new ScanParams().match("*3"));
        ScanResult<Tuple> expected = new ScanResult<>("0",
                Collections.singletonList(new Tuple("a3", 3.0)));
        assertThat(result.getCursor()).isEqualTo(expected.getCursor());
        assertThat(result.getResult()).isEqualTo(expected.getResult());
    }

    @TestTemplate
    public void testZScanAll (Jedis jedis) {
        ScanResult<Tuple> result = jedis.zscan(ZSET_KEY, "0", new ScanParams());
        ScanResult<Tuple> expected = new ScanResult<>("0",
                Arrays.asList(new Tuple("a0", 0.0),
                        new Tuple("a1", 1.0),
                        new Tuple("a2", 2.0),
                        new Tuple("a3", 3.0),
                        new Tuple("a4", 4.0),
                        new Tuple("a5", 5.0),
                        new Tuple("a6", 6.0),
                        new Tuple("a7", 7.0),
                        new Tuple("a8", 8.0),
                        new Tuple("a9", 9.0)));
        assertThat(result.getCursor()).isEqualTo(expected.getCursor());
        assertThat(result.getResult()).isEqualTo(expected.getResult());
    }
}
