package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.ExpiryOption;
import redis.clients.jedis.util.SafeEncoder;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class HashValuesExpirationTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.hset("mykey", "field1", "hello");
        jedis.hset("mykey", "field2", "world");
    }

    @TestTemplate
    void hexpireNonExistingKey(Jedis jedis) {
        List<Long> hexpire = jedis.hexpire("no-key", 20,
                ExpiryOption.NX, "field1", "field2");
        assertThat(hexpire).containsExactly(-2L, -2L);
    }

    @TestTemplate
    void hpexpireNonExistingKey(Jedis jedis) {
        List<Long> hexpire = jedis.hpexpire("no-key", 20,
                ExpiryOption.NX, "field1", "field2");
        assertThat(hexpire).containsExactly(-2L, -2L);
    }

    @TestTemplate
    void hexpireExistingKey(Jedis jedis) throws InterruptedException {
        List<Long> hexpire = jedis.hexpire("mykey", 1, "field1", "field2", "field3");
        assertThat(hexpire).containsExactly(1L, 1L, -2L);
        Thread.sleep(2000);
        assertThat(jedis.hgetAll("mykey")).isEmpty();
    }

    @TestTemplate
    void hpexpireExistingKey(Jedis jedis) throws InterruptedException {
        List<Long> hexpire = jedis.hpexpire("mykey", 10, "field1", "field2", "field3");
        assertThat(hexpire).containsExactly(1L, 1L, -2L);
        Thread.sleep(20);
        assertThat(jedis.hgetAll("mykey")).isEmpty();
    }


    @TestTemplate
    void hexpirePartial(Jedis jedis) throws InterruptedException {
        assertThat(jedis.hlen("mykey")).isEqualTo(2);
        List<Long> hexpire = jedis.hexpire("mykey", 1, "field1");
        assertThat(hexpire).containsExactly(1L);
        Thread.sleep(2000);
        assertThat(jedis.hkeys("mykey")).containsExactly("field2");
    }

    @TestTemplate
    void hpexpirePartial(Jedis jedis) throws InterruptedException {
        assertThat(jedis.hlen("mykey")).isEqualTo(2);
        List<Long> hexpire = jedis.hpexpire("mykey", 10, "field1");
        assertThat(hexpire).containsExactly(1L);
        Thread.sleep(20);
        assertThat(jedis.hkeys("mykey")).containsExactly("field2");
    }

    @TestTemplate
    void hpersistPartial(Jedis jedis) throws InterruptedException {
        jedis.hexpire("mykey", 300, "field1", "field2");
        Thread.sleep(1000);
        Set<Long> httl = new HashSet<>(jedis.httl("mykey", "field1", "field2"));
        assertThat(httl).size().isEqualTo(1);
        assertThat(httl.iterator().next()).isBetween(200L, 299L);
        jedis.hpersist("mykey", "field2");
        List<Long> httlList = jedis.httl("mykey", "field1", "field2");
        assertThat(httlList).size().isEqualTo(2);
        assertThat(httlList.get(1)).isEqualTo(-1L);
        assertThat(httlList.get(0)).isGreaterThan(200);
    }

    @TestTemplate
    void hpersistNonExistent(Jedis jedis) {
        List<Long> result = jedis.hpersist("non-existing", "field2");
        assertThat(result).containsExactly(-2L);
    }

    @TestTemplate
    void hpersistFields(Jedis jedis) {
        jedis.hexpire("mykey", 100, "field1");
        List<Long> result = jedis.hpersist("mykey", "field1", "field2", "field3");
        assertThat(result).containsExactly(1L, -1L, -2L);
    }

    @TestTemplate
    void hexpireNonExisting(Jedis jedis) {
        assertThat(jedis.hexpireTime("non-existing", "field3"))
                .containsExactly(-2L);
        assertThat(jedis.hexpireTime("mykey", "field3"))
                .containsExactly(-2L);
        assertThat(jedis.hexpireTime("mykey", "field2"))
                .containsExactly(-1L);
    }

    @TestTemplate
    void hexpireAt(Jedis jedis) {
        long expireAt = Long.parseLong(jedis.time().get(0)) + 1234;
        jedis.hexpireAt("mykey", expireAt, "field2");
        assertThat(jedis.hexpireTime("mykey", "field1", "field2"))
                .containsExactly(-1L, expireAt);
    }

    @TestTemplate
    public void hpexpireNonExisting(Jedis jedis) {
        assertThat(jedis.hpexpireTime("non-existing", "field3"))
                .containsExactly(-2L);
        assertThat(jedis.hpexpireTime("mykey", "field3"))
                .containsExactly(-2L);
        assertThat(jedis.hpexpireTime("mykey", "field2"))
                .containsExactly(-1L);
    }

    @TestTemplate
    public void hpexpireAt(Jedis jedis) {
        List<String> time = jedis.time();
        long expireAt = Long.parseLong(time.get(0)) * 1000
                + Long.parseLong(time.get(1))
                + 1234567;
        jedis.hpexpireAt("mykey", expireAt, "field2");
        assertThat(jedis.hpexpireTime("mykey", "field1", "field2"))
                .containsExactly(-1L, expireAt);
    }

    @TestTemplate
    void notEnoughArguments(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        Stream.of(
                (Callable<?>) () -> jedis.hexpire("mykey", 1),
                (Callable<?>) () -> jedis.hpexpire("mykey", 20),
                () -> jedis.hexpireTime("mykey"),
                () -> jedis.hpexpireTime("mykey"),
                () -> jedis.httl("mykey"),
                () -> jedis.hpttl("mykey")
        ).forEach(v -> softly
                .assertThatThrownBy(v::call)
                .hasMessageContaining("ERR wrong number of arguments"));
        softly.assertAll();
    }

    @TestTemplate
    void fieldsArgumentIsMissing(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        Stream.of("hexpire", "hpexpire", "hexpireat", "hpexpireat", "httl", "hpttl",
                        "hexpiretime", "hpexpiretime", "hpersist")
                .forEach(
                        cmd ->
                                softly.assertThatThrownBy(() -> jedis.sendCommand(
                                                () -> SafeEncoder.encode(cmd),
                                                SafeEncoder.encode("mykey"),
                                                SafeEncoder.encode("1"),
                                                SafeEncoder.encode("a"),
                                                SafeEncoder.encode("b"),
                                                SafeEncoder.encode("c")))
                                        .hasMessageContaining("ERR Mandatory argument FIELDS is missing " +
                                                "or not at the right position"));
        softly.assertAll();
    }

    @TestTemplate
    void hpExpireWithParameters(Jedis jedis) {
        List<Long> hexpire = jedis.hexpire("mykey", 20,
                "field1", "field2");
        assertThat(hexpire).containsExactly(1L, 1L);
        hexpire = jedis.hexpire("mykey", 40,
                ExpiryOption.NX, "field1", "field2");
        assertThat(hexpire).containsExactly(0L, 0L);
    }

    @TestTemplate
    void httlNonExistingKey(Jedis jedis) {
        List<Long> hexpire = jedis.httl("no-key", "field1", "field2");
        assertThat(hexpire).containsExactly(-2L, -2L);
    }

    @TestTemplate
    void httlNonExisingField(Jedis jedis) {
        List<Long> hexpire = jedis.httl("mykey", "field1", "field2", "field3");
        assertThat(hexpire).containsExactly(-1L, -1L, -2L);
    }


    @TestTemplate
    void hpttl(Jedis jedis) {
        jedis.hexpire("mykey", 10, "field1");
        List<Long> hexpire = jedis.hpttl("mykey", "field1");
        assertThat(hexpire).hasSize(1);
        assertThat(hexpire.get(0)).isGreaterThan(1000);
    }

    @TestTemplate
    void hexpireAtNonExisting(Jedis jedis) {
        List<Long> result = jedis.hexpireAt("foo", 1, "field1");
        assertThat(result).containsExactly(-2L);
    }

    @TestTemplate
    void hexpireAtFields(Jedis jedis) {
        jedis.hexpire("mykey", 100, "field1");
        List<Long> result = jedis.hexpireAt("mykey", 200, ExpiryOption.NX,
                "field1", "field2", "field3");
        assertThat(result).containsExactly(0L, 2L, -2L);
    }

    @TestTemplate
    void hexpireAtFieldsOverride(Jedis jedis) {
        jedis.hexpire("mykey", 100, "field1");
        long expireAt = Long.parseLong(jedis.time().get(0)) + 60;
        List<Long> result = jedis.hexpireAt("mykey", expireAt,
                "field1", "field2", "field3");
        assertThat(result).containsExactly(1L, 1L, -2L);
    }

    @TestTemplate
    void hexpireAtInThePast(Jedis jedis) {
        long expireAt = Long.parseLong(jedis.time().get(0)) - 60;
        List<Long> result = jedis.hexpireAt("mykey", expireAt,
                "field1");
        assertThat(result).containsExactly(2L);
        assertThat(jedis.hexists("mykey", "field1")).isFalse();
    }

    @TestTemplate
    void hexpireWithZeroTime(Jedis jedis) {
        List<Long> result = jedis.hexpire("mykey", 0, "field1");
        assertThat(result).containsExactly(2L);
        assertThat(jedis.hexists("mykey", "field1")).isFalse();
    }

    @TestTemplate
    void hexpireWithNegativeTime(Jedis jedis) {
        assertThatThrownBy(() -> jedis.hexpire("mykey", -1, "field1"))
                .hasMessageContaining("must be >= 0");
    }

    @TestTemplate
    public void lazyExpire(Jedis jedis) throws InterruptedException {
        /*After small period of time, hmaps don't disappear on their own.
         * COUNT does not care about expired fields */
        do {
            jedis.del("myhash");
            jedis.hset("myhash", "f1", "v1");
            jedis.hset("myhash", "f2", "v2");
            jedis.hset("myhash", "f3", "v3");
            jedis.hpexpire("myhash", 1, "f1", "f2", "f3");
            Thread.sleep(2);
            //Sometimes "real" Redis is too fast with active expire, so
            //we wait for a convenient chance
        } while (!jedis.exists("myhash"));
        assertThat(jedis.hlen("myhash")).isEqualTo(3);
        assertThat(jedis.hexists("myhash", "f1")).isFalse();
        assertThat(jedis.hlen("myhash")).isEqualTo(2);

        assertThat(jedis.hget("myhash", "f2")).isNull();
        assertThat(jedis.hlen("myhash")).isEqualTo(1);

        assertThat(jedis.hstrlen("myhash", "f3")).isZero();
        assertThat(jedis.hlen("myhash")).isZero();
        assertThat(jedis.exists("myhash")).isFalse();
    }

    @TestTemplate
    public void activeExpire(Jedis jedis) throws InterruptedException {
        /*Eventually, hash with all the expired fields disappears */
        jedis.hset("myhash", "f1", "v1");
        jedis.hset("myhash", "f2", "v2");
        jedis.hset("myhash", "f3", "v3");
        jedis.hpexpire("myhash", 1, "f1", "f2", "f3");
        Thread.sleep(2);
        assertThat(jedis.exists("myhash")).isTrue();
        Awaitility.await().until(() -> !jedis.exists("myhash"));
    }

}
