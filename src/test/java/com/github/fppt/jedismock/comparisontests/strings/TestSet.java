package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ComparisonBase.class)
public class TestSet {

    private static final String SET_KEY = "my_simple_key";
    private static final String SET_VALUE = "my_simple_value";
    private static final String SET_ANOTHER_VALUE = "another_value";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.del(SET_KEY);
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
        assertEquals("foo", jedis.get("xx"));
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
    } //--------------------------------------------

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

    private void testSetValueExpires(Jedis jedis, SetParams setParams) throws InterruptedException {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, setParams));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1100);
        assertNull(jedis.get(SET_KEY));
    }

    private void testSetNXWithParams(Jedis jedis, SetParams setParams) {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, setParams));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertNull(jedis.set(SET_KEY, SET_ANOTHER_VALUE, setParams));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertEquals(1, jedis.del(SET_KEY));
    }

    private void testSetXXWithParams(Jedis jedis, SetParams setParams) {
        assertNull(jedis.set(SET_KEY, SET_VALUE, setParams));
        assertNull(jedis.get(SET_KEY));
        assertEquals("OK", jedis.set(SET_KEY, SET_ANOTHER_VALUE));
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, setParams));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertEquals(1, jedis.del(SET_KEY));
    }
}
