package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.List;

import static java.lang.Long.parseLong;
import static java.lang.Math.abs;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class ServerOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenUsingFlushall_EnsureEverythingIsDeleted(Jedis jedis) {
        String key = "my-super-special-key";
        String value = "my-not-so-special-value";

        jedis.set(key, value);
        assertThat(jedis.get(key)).isEqualTo(value);

        jedis.flushAll();
        assertThat(jedis.get(key)).isNull();
    }

    @TestTemplate
    public void whenUsingFlushdb_EnsureEverythingIsDeleted(Jedis jedis) {
        String key = "my-super-special-key";
        String value = "my-not-so-special-value";

        jedis.set(key, value);
        assertThat(jedis.get(key)).isEqualTo(value);

        jedis.flushDB();
        assertThat(jedis.get(key)).isNull();
    }

    @TestTemplate
    public void whenCountingKeys_EnsureExpiredKeysAreNotCounted(Jedis jedis) throws InterruptedException {
        jedis.hset("test", "key", "value");
        jedis.expire("test", 1L);
        assertThat(jedis.dbSize()).isEqualTo(1);
        Thread.sleep(2000);
        assertThat(jedis.dbSize()).isEqualTo(0);
    }

    @TestTemplate
    public void whenGettingInfo_EnsureSomeDateIsReturned(Jedis jedis) {
        assertThat(jedis.info()).isNotNull();
    }

    @TestTemplate
    public void timeReturnsCurrentTime(Jedis jedis) {
        long currentTime = System.currentTimeMillis() / 1000;
        List<String> time = jedis.time();
        //We believe that results difference will be within one second
        assertThat(abs(currentTime - parseLong(time.get(0)))).isLessThan(2);
        //Microseconds are correct integer value
        Long.parseLong(time.get(1));
    }

    @TestTemplate
    public void dbSizeReturnsCount(Jedis jedis) {
        String HASH = "hash";
        String FIELD_1 = "field1";
        String VALUE_1 = "value1";
        String FIELD_2 = "field2";
        String VALUE_2 = "value2";
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        jedis.set(FIELD_1, VALUE_1);

        long result = jedis.dbSize();

        assertThat(result).isEqualTo(2);
    }
}
