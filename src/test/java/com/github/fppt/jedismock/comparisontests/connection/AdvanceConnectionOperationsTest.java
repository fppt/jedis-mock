package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ComparisonBase.class)
public class AdvanceConnectionOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenChangingBetweenRedisDBS_EnsureChangesAreMutuallyExclusive(Jedis jedis) {
        String key1 = "k1";
        String key2 = "k2";

        String val1 = "v1";
        String val2 = "v2";
        String val3 = "v3";

        //Mess With Default Cluster
        jedis.set(key1, val1);
        jedis.set(key2, val2);
        assertEquals(val1, jedis.get(key1));
        assertEquals(val2, jedis.get(key2));

        //Change to new DB
        jedis.select(2);
        assertNull(jedis.get(key1));
        assertNull(jedis.get(key2));

        jedis.set(key1, val3);
        jedis.set(key2, val3);
        assertEquals(val3, jedis.get(key1));
        assertEquals(val3, jedis.get(key2));

        //Change back and make sure original is unchanged
        jedis.select(0);
        assertEquals(val1, jedis.get(key1));
        assertEquals(val2, jedis.get(key2));
    }
}
