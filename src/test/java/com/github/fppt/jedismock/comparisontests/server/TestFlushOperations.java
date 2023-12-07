package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestFlushOperations {
    @TestTemplate
    void whenFlushDBCalled_ensureKeysAreErased(Jedis jedis) {
        jedis.set("foo", "val1");
        jedis.set("bar", "val2");
        assertThat(jedis.dbSize()).isEqualTo(2);
        jedis.flushDB();
        assertThat(jedis.dbSize()).isEqualTo(0);
    }

    @TestTemplate
    void whenFlushAllCalled_ensureAllDatabasesAreErased(Jedis jedis) {
        for (int i = 0; i < 3; i++) {
            jedis.select(i);
            jedis.set("foo" + i, "val1");
            jedis.set("bar" + i, "val2");
            assertThat(jedis.dbSize()).isEqualTo(2);
        }
        jedis.flushAll();
        for (int i = 0; i < 3; i++) {
            jedis.select(i);
            assertThat(jedis.dbSize()).isEqualTo(0);
        }
    }
}
