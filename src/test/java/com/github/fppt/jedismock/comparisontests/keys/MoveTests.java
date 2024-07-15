package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class MoveTests {
    private final String srcKey = "first";
    private final int dstDb = 11;
    private final String val = "abracadabra";


    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void successfulMove(Jedis jedis) {
        jedis.setex(srcKey, 100, val);
        long expireTime = jedis.pexpireTime(srcKey);
        assertThat(jedis.move(srcKey, dstDb)).isEqualTo(1);
        jedis.select(dstDb);
        assertThat(jedis.get(srcKey)).isEqualTo(val);
        assertThat(jedis.pexpireTime(srcKey)).isEqualTo(expireTime);
    }

    @TestTemplate
    public void copyOfNonExistent(Jedis jedis) {
        assertThat(jedis.move("nonexistent", dstDb)).isZero();
    }

    @TestTemplate
    public void unsuccessfulMove(Jedis jedis) {
        jedis.select(0);
        jedis.set(srcKey, val);
        jedis.select(dstDb);
        jedis.set(srcKey, "oldValue");
        jedis.select(0);
        assertThat(jedis.move(srcKey, dstDb)).isZero();
        jedis.select(dstDb);
        assertThat(jedis.get(srcKey)).isEqualTo("oldValue");
    }
}
