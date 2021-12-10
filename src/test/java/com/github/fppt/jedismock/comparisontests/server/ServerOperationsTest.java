package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class ServerOperationsTest {

    private final String HASH = "hash";
    private final String FIELD_1 = "field1";
    private final String VALUE_1 = "value1";
    private final String FIELD_2 = "field2";
    private final String VALUE_2 = "value2";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenUsingFlushall_EnsureEverythingIsDeleted(Jedis jedis) {
        String key = "my-super-special-key";
        String value = "my-not-so-special-value";

        jedis.set(key, value);
        assertEquals(value, jedis.get(key));

        jedis.flushAll();
        assertNull(jedis.get(key));
    }

    @TestTemplate
    public void whenUsingFlushdb_EnsureEverythingIsDeleted(Jedis jedis) {
        String key = "my-super-special-key";
        String value = "my-not-so-special-value";

        jedis.set(key, value);
        assertEquals(value, jedis.get(key));

        jedis.flushDB();
        assertNull(jedis.get(key));
    }

    @TestTemplate
    public void whenCountingKeys_EnsureExpiredKeysAreNotCounted(Jedis jedis) throws InterruptedException {
        jedis.hset("test", "key", "value");
        jedis.expire("test", 1L);
        assertEquals(1, jedis.dbSize());
        Thread.sleep(2000);
        assertEquals(0, jedis.dbSize());
    }

    @TestTemplate
    public void whenGettingInfo_EnsureSomeDateIsReturned(Jedis jedis) {
        assertNotNull(jedis.info());
    }

    @TestTemplate
    public void timeReturnsCurrentTime(Jedis jedis) {
        long currentTime = System.currentTimeMillis() / 1000;
        List<String> time = jedis.time();
        //We believe that results difference will be within one second
        assertTrue(Math.abs(currentTime - Long.parseLong(time.get(0))) < 2);
        //Microseconds are correct integer value
        Long.parseLong(time.get(1));
    }

    @TestTemplate
    public void dbSizeReturnsCount(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        jedis.set(FIELD_1, VALUE_1);

        long result = jedis.dbSize();

        assertEquals(2, result);
    }
}
