package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class HKeysOperation {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void hkeysUnknownKey(Jedis jedis) {
        Set<String> res = jedis.hkeys("foo");
        assertTrue(res.isEmpty());
    }

    @TestTemplate
    void hvalsUnknownKey(Jedis jedis) {
        List<String> res = jedis.hvals("foo");
        assertTrue(res.isEmpty());
    }

    @TestTemplate
    void hlenUnknownKey(Jedis jedis) {
        long hlen = jedis.hlen("foo");
        assertEquals(0, hlen);
    }

    @TestTemplate
    void hGetAllUnknownKey(Jedis jedis) {
        Map<String, String> result = jedis.hgetAll("foo");
        assertTrue(result.isEmpty());
    }
}
