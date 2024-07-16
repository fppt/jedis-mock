package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.util.stream.LongStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestSet {

    private static final String SET_KEY = "my_simple_key";
    private static final String SET_VALUE = "my_simple_value";
    private static final String SET_ANOTHER_VALUE = "another_value";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.flushDB();
    }

    // SET key value NX
    @TestTemplate
    public void testSetNX(Jedis jedis) {
        testSetNXWithParams(jedis, new SetParams().nx());
    }

    // SET key value XX
    @TestTemplate
    public void testSetXX(Jedis jedis) {
        testSetXXWithParams(jedis, new SetParams().xx());
    }

    @TestTemplate
    void testSetXXKey(Jedis jedis) {
        jedis.set("xx", "foo");
        assertThat(jedis.get("xx")).isEqualTo("foo");
    }

    // SET key value EX s
    @TestTemplate
    public void testSetEX(Jedis jedis) throws InterruptedException {
        testSetValueExpires(jedis, new SetParams().ex(1L));
    }

    // SET key value PX ms
    @TestTemplate
    public void testSetPX(Jedis jedis) throws InterruptedException {
        testSetValueExpires(jedis, new SetParams().px(1000L));
    }

    // SET key value EX s NX
    @TestTemplate
    public void testSetEXNXexpires(Jedis jedis) throws InterruptedException {
        testSetValueExpires(jedis, new SetParams().ex(1L).nx());
    }

    @TestTemplate
    public void testSetEXNXnotexists(Jedis jedis) {
        testSetNXWithParams(jedis, new SetParams().ex(1L).nx());
    }

    // SET key value PX ms NX
    @TestTemplate
    public void testSetPXNXexpires(Jedis jedis) throws InterruptedException {
        testSetValueExpires(jedis, new SetParams().px(1000L).nx());
    }

    @TestTemplate
    public void testSetPXNXnotexists(Jedis jedis) {
        testSetNXWithParams(jedis, new SetParams().px(1000L).nx());
    }

    // SET key value EX s XX
    @TestTemplate
    public void testSetEXXXexpires(Jedis jedis) throws InterruptedException {
        jedis.set(SET_KEY, SET_ANOTHER_VALUE);
        testSetValueExpires(jedis, new SetParams().ex(1L).xx());
    }

    @TestTemplate
    public void testSetEXXXnotexists(Jedis jedis) {
        testSetXXWithParams(jedis, new SetParams().ex(1L).xx());
    }

    // SET key value PX ms XX
    @TestTemplate
    public void testSetPXXXexpires(Jedis jedis) throws InterruptedException {
        jedis.set(SET_KEY, SET_ANOTHER_VALUE);
        testSetValueExpires(jedis, new SetParams().px(1000L).xx());
    }

    @TestTemplate
    public void testSetPXXXnotexists(Jedis jedis) {
        testSetXXWithParams(jedis, new SetParams().px(1000L).xx());
    }

    @TestTemplate
    public void testSetBadExpiration(Jedis jedis) {
        SoftAssertions softly = new SoftAssertions();
        LongStream.of(
                9223370399119966L,
                9223372036854776L,
                10000000000000000L,
                18446744073709561L,
                -9223372036854776L,
                -9999999999999999L,
                -100L,
                0L
        ).forEach(
                v -> softly.assertThatThrownBy(() ->
                                jedis.set("foo", "bar", SetParams.setParams().ex(v)))
                        .hasMessage("ERR invalid expire time in 'set' command")
        );
        softly.assertAll();
    }

    @TestTemplate
    public void keepTTL(Jedis jedis) {
        long exat = System.currentTimeMillis() / 1000 + 1050;
        jedis.set(SET_KEY, SET_VALUE);
        jedis.expireAt(SET_KEY, exat);
        jedis.set(SET_KEY, SET_ANOTHER_VALUE, SetParams.setParams().keepTtl());
        assertThat(jedis.expireTime(SET_KEY)).isEqualTo(exat);
    }

    @TestTemplate
    public void testSetNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.set("foo".getBytes(), msg);
        assertThat(jedis.get("foo".getBytes())).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testSetEmptyString(Jedis jedis) {
        jedis.set("foo", "");
        assertThat(jedis.get("foo")).isEqualTo("");
    }

    private void testSetValueExpires(Jedis jedis, SetParams setParams) throws InterruptedException {
        assertThat(jedis.set(SET_KEY, SET_VALUE, setParams)).isEqualTo("OK");
        assertThat(jedis.get(SET_KEY)).isEqualTo(SET_VALUE);
        Thread.sleep(1100);
        assertThat(jedis.get(SET_KEY)).isNull();
    }

    private void testSetNXWithParams(Jedis jedis, SetParams setParams) {
        assertThat(jedis.set(SET_KEY, SET_VALUE, setParams)).isEqualTo("OK");
        assertThat(jedis.get(SET_KEY)).isEqualTo(SET_VALUE);
        assertThat(jedis.set(SET_KEY, SET_ANOTHER_VALUE, setParams)).isNull();
        assertThat(jedis.get(SET_KEY)).isEqualTo(SET_VALUE);
        assertThat(jedis.del(SET_KEY)).isEqualTo(1);
    }

    private void testSetXXWithParams(Jedis jedis, SetParams setParams) {
        assertThat(jedis.set(SET_KEY, SET_VALUE, setParams)).isNull();
        assertThat(jedis.get(SET_KEY)).isNull();
        assertThat(jedis.set(SET_KEY, SET_ANOTHER_VALUE)).isEqualTo("OK");
        assertThat(jedis.set(SET_KEY, SET_VALUE, setParams)).isEqualTo("OK");
        assertThat(jedis.get(SET_KEY)).isEqualTo(SET_VALUE);
        assertThat(jedis.del(SET_KEY)).isEqualTo(1);
    }

    @TestTemplate
    void testSetExat(Jedis jedis) {
        long exat = System.currentTimeMillis() / 1000 + 1050;
        jedis.set(SET_KEY, SET_VALUE, SetParams.setParams().exAt(exat));
        assertThat(jedis.expireTime(SET_KEY)).isEqualTo(exat);
    }

    @TestTemplate
    void testSetPExat(Jedis jedis) {
        long pxat = System.currentTimeMillis() + 100500;
        jedis.set(SET_KEY, SET_VALUE, SetParams.setParams().pxAt(pxat));
        assertThat(jedis.pexpireTime(SET_KEY)).isEqualTo(pxat);
    }

}
