package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

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

        assertThat(jedis.hstrlen("myhash", "f1")).isEqualTo(10);
        assertThat(jedis.hstrlen("myhash", "f2")).isEqualTo(2);
        assertThat(jedis.hstrlen("myhash", "f3")).isEqualTo(4);
        assertThat(jedis.hstrlen("myhash", "no_such_field")).isEqualTo(0);
    }

    @TestTemplate
    public void hstrlenReturnsZeroForNonExistent(Jedis jedis) {
        assertThat(jedis.hstrlen("no_such_hash", "no_such_field")).isEqualTo(0);
    }
}
