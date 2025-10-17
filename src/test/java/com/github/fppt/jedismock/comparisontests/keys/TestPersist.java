package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestPersist {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void testPersistExistingWithTTL(Jedis jedis) throws Exception {
        jedis.psetex("a", 300, "v");
        assertThat(jedis.ttl("a")).isLessThanOrEqualTo(300);
        assertThat(jedis.persist("a")).isEqualTo(1);
        assertThat(jedis.ttl("a")).isEqualTo(-1);
        Thread.sleep(500);
        //Check that the value is still there
        assertThat(jedis.get("a")).isEqualTo("v");
    }

    @TestTemplate
    public void testPersistExistingWithoutTTL(Jedis jedis) {
        jedis.set("key", "value");
        assertThat(jedis.persist("key")).isEqualTo(0);
        assertThat(jedis.ttl("key")).isEqualTo(-1);
        assertThat(jedis.get("key")).isEqualTo("value");
    }

    @TestTemplate
    public void testPersistNotExisting(Jedis jedis) {
        assertThat(jedis.persist("foo")).isEqualTo(0);
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }
}
