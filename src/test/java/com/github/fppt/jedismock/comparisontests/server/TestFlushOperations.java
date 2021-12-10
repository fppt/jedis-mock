package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestFlushOperations {
    @TestTemplate
    void whenFlushDBCalled_ensureKeysAreErased(Jedis jedis) {
        jedis.set("foo", "val1");
        jedis.set("bar", "val2");
        assertEquals(2, jedis.dbSize());
        jedis.flushDB();
        assertEquals(0, jedis.dbSize());
    }

    @TestTemplate
    void whenFlushAllCalled_ensureAllDatabasesAreErased(Jedis jedis) {
        for (int i = 0; i < 3; i++) {
            jedis.select(i);
            jedis.set("foo" + i, "val1");
            jedis.set("bar" + i, "val2");
            assertEquals(2, jedis.dbSize());
        }
        jedis.flushAll();
        for (int i = 0; i < 3; i++) {
            jedis.select(i);
            assertEquals(0, jedis.dbSize());
        }
    }
}
