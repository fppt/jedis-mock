package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class CopyTests {
    private final String srcKey = "first";
    private final String dstKey = "second";
    private final int dstDb = 11;
    private final String val = "abracadabra";


    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void successfulCopySameDb(Jedis jedis) {
        jedis.setex(srcKey, 100, val);
        assertThat(jedis.copy(srcKey, dstKey, false)).isTrue();
        assertThat(jedis.get(dstKey)).isEqualTo(val);
        long expireTime = jedis.pexpireTime(srcKey);
        assertThat(jedis.pexpireTime(dstKey)).isEqualTo(expireTime);
    }

    @TestTemplate
    public void successfulCopyDifferentDb(Jedis jedis) {
        jedis.setex(srcKey, 100, val);
        assertThat(jedis.copy(srcKey, dstKey, dstDb, false)).isTrue();
        long expireTime = jedis.pexpireTime(srcKey);
        jedis.select(dstDb);
        assertThat(jedis.get(dstKey)).isEqualTo(val);
        assertThat(jedis.pexpireTime(dstKey)).isEqualTo(expireTime);
    }

    @TestTemplate
    public void copyOfNonExistent(Jedis jedis) {
        assertThat(jedis.copy("nonexistent", dstKey, dstDb, false)).isFalse();
    }

    @TestTemplate
    public void unsuccessfulCopy(Jedis jedis) {
        jedis.set(srcKey, val);
        jedis.set(dstKey, "oldValue");
        assertThat(jedis.copy(srcKey, dstKey, false)).isFalse();
        assertThat(jedis.get(dstKey)).isEqualTo("oldValue");
    }

    @TestTemplate
    public void copyWithReplace(Jedis jedis) {
        jedis.set(srcKey, val);
        jedis.set(dstKey, "oldValue");
        assertThat(jedis.copy(srcKey, dstKey, true)).isTrue();
        assertThat(jedis.get(dstKey)).isEqualTo(val);
    }

    @TestTemplate
    public void copyDoesNotCreateAnExpireIfItDoesNotExist(Jedis jedis) {
        jedis.set("mykey", "foobar");
        assertThat(jedis.ttl("mykey")).isEqualTo(-1);
        jedis.copy("mykey", "mynewkey", true);
        assertThat(jedis.ttl("mynewkey")).isEqualTo(-1);
        assertThat(jedis.get("mynewkey")).isEqualTo("foobar");
    }
}
