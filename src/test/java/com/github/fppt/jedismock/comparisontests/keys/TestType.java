package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestType {
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushDB();
        jedis.set("key", "string");
        jedis.lpush("lkey", "value1", "value2");
        jedis.sadd("skey", "val1", "val2");
        jedis.zadd("zkey", 1, "foo");
        jedis.hset("hkey", "k", "v");
        jedis.setbit("bitmap", 22, true);
        jedis.pfadd("hll", "foo");
    }

    @TestTemplate
    void type(Jedis jedis) {
        assertThat(jedis.type("not.exists")).isEqualTo("none");
        assertThat(jedis.type("key")).isEqualTo("string");
        assertThat(jedis.type("lkey")).isEqualTo("list");
        assertThat(jedis.type("skey")).isEqualTo("set");
        assertThat(jedis.type("zkey")).isEqualTo("zset");
        assertThat(jedis.type("hkey")).isEqualTo("hash");
        assertThat(jedis.type("bitmap")).isEqualTo("string");
        assertThat(jedis.type("hll")).isEqualTo("string");
    }

    @TestTemplate
    void typeRespectsTTL(Jedis jedis) throws InterruptedException {
        jedis.pexpire("key", 50);
        Thread.sleep(100);
        assertThat(jedis.type("key")).isEqualTo("none");
    }
}
