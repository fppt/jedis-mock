package com.github.fppt.jedismock.comparisontests.keys;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.ExpiryOption;

import java.util.List;
import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestExpiration {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenGettingKeys_EnsureExpiredKeysAreNotReturned(Jedis jedis) throws InterruptedException {
        jedis.hset("test", "key", "value");
        jedis.expire("test", 1L);
        assertThat(jedis.keys("*")).containsExactly("test");
        Thread.sleep(2000);
        assertThat(jedis.keys("*")).isEmpty();
    }

    @TestTemplate
    public void hashExpires(Jedis jedis) throws InterruptedException {
        String key = "mykey";
        String subkey = "mysubkey";

        jedis.hsetnx(key, subkey, "a");
        jedis.expire(key, 1L);

        Thread.sleep(2000);

        String result = jedis.hget(key, subkey);

        assertThat(result).isNull();
    }

    @TestTemplate
    public void multipleExpire(Jedis jedis) {
        jedis.set("foo1", "bar");
        jedis.set("foo2", "bar");
        jedis.expire("foo1", 0);
        jedis.expire("foo2", 0);
        assertThat(jedis.dbSize()).isEqualTo(0);
    }

    @TestTemplate
    public void expireWithParams(Jedis jedis) {
        jedis.set("mykey", "Hello World");
        assertThat(jedis.ttl("mykey")).isEqualTo(-1);
        assertThat(jedis.expire("mykey", 10, ExpiryOption.XX)).isZero();
        assertThat(jedis.ttl("mykey")).isEqualTo(-1);
        assertThat(jedis.expire("mykey", 10, ExpiryOption.NX)).isEqualTo(1);
        assertThat(jedis.ttl("mykey")).isBetween(8L, 10L);
        assertThat(jedis.expire("mykey", 5, ExpiryOption.GT)).isEqualTo(0);
        assertThat(jedis.ttl("mykey")).isBetween(8L, 10L);
        assertThat(jedis.expire("mykey", 5, ExpiryOption.LT)).isEqualTo(1);
        assertThat(jedis.ttl("mykey")).isBetween(3L, 5L);
    }

    @TestTemplate
    public void expireLTOnAKeyWithoutTTL(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.expire("foo", 100, ExpiryOption.LT)).isEqualTo(1);
        assertThat(jedis.ttl("foo")).isBetween(98L, 100L);
    }

    @TestTemplate
    public void expireGTOnAKeyWithoutTTL(Jedis jedis) {
        jedis.set("foo", "bar");
        assertThat(jedis.expire("foo", 100, ExpiryOption.GT)).isEqualTo(0);
        assertThat(jedis.ttl("foo")).isEqualTo(-1);
    }

    @TestTemplate
    public void expireNonExistedKey(Jedis jedis) {
        assertThat(jedis.expire("nonexistedkey", 100)).isZero();
        assertThat(jedis.expire("nonexistedkey", 100, ExpiryOption.NX)).isZero();
        assertThat(jedis.expire("nonexistedkey", 100, ExpiryOption.XX)).isZero();
        assertThat(jedis.expire("nonexistedkey", 100, ExpiryOption.GT)).isZero();
        assertThat(jedis.expire("nonexistedkey", 100, ExpiryOption.LT)).isZero();
    }

    @TestTemplate
    public void expireTime(Jedis jedis) {
        assertThat(jedis.expireTime("mykey")).isEqualTo(-2);
        jedis.set("mykey", "myvalue");
        assertThat(jedis.expireTime("mykey")).isEqualTo(-1);
        long expireAt = Long.parseLong(jedis.time().get(0)) + 1234;
        jedis.expireAt("mykey", expireAt);
        assertThat(jedis.expireTime("mykey")).isEqualTo(expireAt);
    }

    @TestTemplate
    public void pExpireTime(Jedis jedis) {
        assertThat(jedis.pexpireTime("mykey")).isEqualTo(-2);
        jedis.set("mykey", "myvalue");
        assertThat(jedis.pexpireTime("mykey")).isEqualTo(-1);
        List<String> time = jedis.time();
        long expireAt = Long.parseLong(time.get(0)) * 1000
                + Long.parseLong(time.get(1))
                + 1234567;
        jedis.pexpireAt("mykey", expireAt);
        assertThat(jedis.pexpireTime("mykey")).isEqualTo(expireAt);
    }

    @TestTemplate
    public void badExpireTime(Jedis jedis) {
        jedis.set("foo", "bar");
        SoftAssertions softly = new SoftAssertions();
        LongStream.of(
                9223370399119966L,
                9223372036854776L,
                10000000000000000L,
                18446744073709561L,
                -9223372036854776L,
                -9999999999999999L
        ).forEach(
                v -> softly.assertThatThrownBy(() ->
                                jedis.expire("foo", v))
                        .hasMessage("ERR invalid expire time in 'expire' command")
        );
        softly.assertAll();
    }

    @TestTemplate
    public void smallNegativeExpireTime(Jedis jedis) {
        jedis.set("foo", "bar");
        jedis.expire("foo", -100);
        assertThat(jedis.ttl("foo")).isEqualTo(-2);
    }

    @TestTemplate
    public void expireAtNonExistingKey(Jedis jedis) {
        assertThat(jedis.expireAt("nonexistedkey", 100)).isZero();
        assertThat(jedis.pexpireAt("nonexistedkey", 100)).isZero();
    }

    @TestTemplate
    public void expireAtTheSameTime(Jedis jedis) {
        jedis.set("foo", "bar");
        long expireAt = Long.parseLong(jedis.time().get(0)) + 4000;
        assertThat(jedis.expireAt("foo", expireAt)).isEqualTo(1);
        assertThat(jedis.expireAt("foo", expireAt)).isEqualTo(1);
    }

    @TestTemplate
    public void expireAtWithParams(Jedis jedis) {
        long expireAt = Long.parseLong(jedis.time().get(0)) + 4000;
        jedis.set("mykey", "Hello World");
        assertThat(jedis.ttl("mykey")).isEqualTo(-1);
        assertThat(jedis.expireAt("mykey", expireAt, ExpiryOption.XX)).isZero();
        assertThat(jedis.expireTime("mykey")).isEqualTo(-1);
        assertThat(jedis.expireAt("mykey", expireAt, ExpiryOption.NX)).isEqualTo(1);
        assertThat(jedis.expireTime("mykey")).isEqualTo(expireAt);
        assertThat(jedis.expireAt("mykey", expireAt, ExpiryOption.GT)).isEqualTo(0);
        assertThat(jedis.expireTime("mykey")).isEqualTo(expireAt);
        assertThat(jedis.expireAt("mykey", expireAt - 1, ExpiryOption.LT)).isEqualTo(1);
        assertThat(jedis.expireTime("mykey")).isEqualTo(expireAt - 1);
    }

}
