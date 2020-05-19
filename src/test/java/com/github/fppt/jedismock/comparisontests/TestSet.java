package com.github.fppt.jedismock.comparisontests;

import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.exception.ParseErrorException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SetParams;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

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
    public void testSetNX(Jedis jedis) throws ParseErrorException {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().nx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertNull(jedis.set(SET_KEY, SET_ANOTHER_VALUE, new SetParams().nx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertEquals(1, jedis.del(SET_KEY));
    }

    // SET key value XX
    @TestTemplate
    public void testSetXX(Jedis jedis) throws ParseErrorException {
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().xx()));
        assertNull(jedis.get(SET_KEY));
        assertEquals("OK", jedis.set(SET_KEY, SET_ANOTHER_VALUE));
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().xx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertEquals(1, jedis.del(SET_KEY));
    }

    // SET key value EX s
    @TestTemplate
    public void testSetEX(Jedis jedis) throws ParseErrorException, InterruptedException {
        assertNull(jedis.get(SET_KEY));
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L)));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1000);
        assertNull(jedis.get(SET_KEY));
    }

    // SET key value PX ms
    @TestTemplate
    public void testSetPX(Jedis jedis) throws ParseErrorException, InterruptedException {
        assertNull(jedis.get(SET_KEY));
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L)));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1000);
        assertNull(jedis.get(SET_KEY));
    }

    // SET key value EX s NX
    @TestTemplate
    public void testSetEXNXexpires(Jedis jedis) throws ParseErrorException, InterruptedException {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L).nx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1000);
        assertNull(jedis.get(SET_KEY));
    }

    @TestTemplate
    public void testSetEXNXnotexists(Jedis jedis) throws ParseErrorException {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L).nx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L).nx()));
        assertEquals(1, jedis.del(SET_KEY));
    }

    // SET key value PX ms NX
    @TestTemplate
    public void testSetPXNXexpires(Jedis jedis) throws ParseErrorException, InterruptedException {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L).nx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1000);
        assertNull(jedis.get(SET_KEY));
    }

    @TestTemplate
    public void testSetPXNXnotexists(Jedis jedis) throws ParseErrorException {
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L).nx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L).nx()));
        assertEquals(1, jedis.del(SET_KEY));
    }

    // SET key value EX s XX
    @TestTemplate
    public void testSetEXXXexpires(Jedis jedis) throws ParseErrorException, InterruptedException {
        jedis.set(SET_KEY, SET_ANOTHER_VALUE);
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L).xx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1000);
        assertNull(jedis.get(SET_KEY));
    }

    @TestTemplate
    public void testSetEXXXnotexists(Jedis jedis) throws ParseErrorException {
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L).xx()));
        assertNull(jedis.set(SET_KEY, SET_ANOTHER_VALUE));
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().ex(1L).xx()));
        assertEquals(1, jedis.del(SET_KEY));
    }

    // SET key value PX ms XX
    @TestTemplate
    public void testSetPXXXexpires(Jedis jedis) throws ParseErrorException, InterruptedException {
        jedis.set(SET_KEY, SET_ANOTHER_VALUE);
        assertEquals("OK", jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L).xx()));
        assertEquals(SET_VALUE, jedis.get(SET_KEY));
        Thread.sleep(1000);
        assertNull(jedis.get(SET_KEY));
    }

    @TestTemplate
    public void testSetPXXXnotexists(Jedis jedis) throws ParseErrorException {
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L).xx()));
        assertNull(jedis.set(SET_KEY, SET_ANOTHER_VALUE));
        assertNull(jedis.set(SET_KEY, SET_VALUE, new SetParams().px(1000L).xx()));
        assertEquals(1, jedis.del(SET_KEY));
    }
}
