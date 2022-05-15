package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertAll;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        assertAll(
                () -> assertEquals("none", jedis.type("not.exists")),
                () -> assertEquals("string", jedis.type("key")),
                () -> assertEquals("list", jedis.type("lkey")),
                () -> assertEquals("set", jedis.type("skey")),
                () -> assertEquals("zset", jedis.type("zkey")),
                () -> assertEquals("hash", jedis.type("hkey")),
                () -> assertEquals("string", jedis.type("bitmap")),
                () -> assertEquals("string", jedis.type("hll"))
        );
    }

    @TestTemplate
    void typeRespectsTTL(Jedis jedis) throws InterruptedException {
        jedis.pexpire("key", 50);
        Thread.sleep(100);
        assertEquals("none", jedis.type("key"));
    }
}
