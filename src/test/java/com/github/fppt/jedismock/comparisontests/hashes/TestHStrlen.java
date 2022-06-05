package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestHStrlen {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
    }

    @TestTemplate
    public void hstrlenReturnsLengthOfFields(Jedis jedis) {
        jedis.hset("myhash", "f1", "HelloWorld");
        jedis.hset("myhash", "f2", "99");
        jedis.hset("myhash", "f3", "-256");

        assertEquals(10, jedis.hstrlen("myhash", "f1"));
        assertEquals(2, jedis.hstrlen("myhash", "f2"));
        assertEquals(4, jedis.hstrlen("myhash", "f3"));
        assertEquals(0, jedis.hstrlen("myhash", "no_such_field"));
    }

    @TestTemplate
    public void hstrlenReturnsZeroForNonExistent(Jedis jedis) {
        assertEquals(0, jedis.hstrlen("no_such_hash", "no_such_field"));
    }
}
