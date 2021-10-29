package com.github.fppt.jedismock;

import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.exception.ParseErrorException;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestRedisOperationExecutor {

    private static final String CRLF = "\r\n";
    private static RedisOperationExecutor executor;

    private static String bulkString(CharSequence param) {
        return "$" + param.length() + CRLF + param.toString() + CRLF;
    }

    private static String array(CharSequence... params) {
        StringBuilder builder = new StringBuilder();
        builder.append('*').append(params.length).append(CRLF);
        for (CharSequence param : params) {
            if (param == null) {
                builder.append("$-1").append(CRLF);
            } else {
                builder.append(bulkString(param));
            }
        }
        return builder.toString();
    }

    private static String nullArray() {
        return "*1" + CRLF + "$-1" + CRLF;
    }

    private void assertCommandEquals(String expect, String command) throws ParseErrorException {
        assertEquals(bulkString(expect), executor.execCommand(RedisCommandParser.parse(command)).toString());
    }

    private void assertCommandEquals(long expect, String command) throws ParseErrorException {
        assertEquals(Response.integer(expect), executor.execCommand(RedisCommandParser.parse(command)));
    }

    private void assertCommandEquals(double expect, String command) throws ParseErrorException {
        assertEquals(Response.doubleValue(expect), executor.execCommand(RedisCommandParser.parse(command)));
    }

    private void assertCommandArrayEquals(String expectedArray, String command) throws ParseErrorException {
        assertEquals(expectedArray, executor.execCommand(RedisCommandParser.parse(command)).toString());
    }

    private void assertCommandNull(String command) throws ParseErrorException {
        assertEquals(Response.NULL, executor.execCommand(RedisCommandParser.parse(command)));
    }

    private void assertCommandOK(String command) throws ParseErrorException {
        assertEquals(Response.OK, executor.execCommand(RedisCommandParser.parse(command)));
    }

    private void assertCommandError(String command) throws ParseErrorException {
        assertEquals('-', executor.execCommand(RedisCommandParser.parse(command)).data()[0]);
    }

    private String del(String key){ return executor.execCommand(RedisCommandParser.parse(array("DEL", key))).toString(); }
    private String set(String key, String value){ return executor.execCommand(RedisCommandParser.parse(array("SET", key, value))).toString(); }

    @BeforeEach
    public void initCommandExecutor() {
        //TODO: Mock out the client here
        Map<Integer, RedisBase> redisBases = new HashMap<>();
        redisBases.put(0, new RedisBase());
        OperationExecutorState state = new OperationExecutorState(null, redisBases);
        executor = new RedisOperationExecutor(state);
    }

    @Test
    public void testSetAndGet() throws ParseErrorException {
        assertCommandNull(array("GET", "ab"));
        assertCommandOK(array("SET", "ab", "abc"));
        assertCommandEquals("abc", array("GET", "ab"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals("abd", array("GET", "ab"));
        assertCommandNull(array("GET", "ac"));
    }

    // SET key value NX
    @Test public void testSetNX() throws ParseErrorException {
        assertCommandOK(array("SET", "key", "value", "NX"));
        assertCommandEquals("value", array("GET", "key"));
        assertCommandNull(array("SET", "key", "value", "NX"));
        assertCommandEquals(1, array("DEL", "key"));
    }

    // SET key value XX
    @Test public void testSetXX() throws ParseErrorException {
        del("key");
        assertCommandNull(array("SET", "key", "value", "XX"));
        assertCommandOK(array("SET", "key", "value"));
        assertCommandOK(array("SET", "key", "value", "XX"));
        assertCommandEquals(1, array("DEL", "key"));
    }

    // SET key value EX s
    @Test public void testSetEX() throws ParseErrorException, InterruptedException {
        assertCommandOK(array("SET", "key", "value", "EX", "1"));
        assertCommandEquals("value", array("GET", "key"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "key"));
    }

    // SET key value PX ms
    @Test public void testSetPX() throws ParseErrorException, InterruptedException {
        assertCommandOK(array("SET", "key", "value", "PX", "1000"));
        assertCommandEquals("value", array("GET", "key"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "key"));
    }

    // SET key value EX s NX
    @Test public void testSetEXNXexpires() throws ParseErrorException, InterruptedException {
        assertCommandOK(array("SET", "key", "value", "EX", "1", "NX"));
        assertCommandEquals("value", array("GET", "key"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "key"));
    }
    @Test public void testSetEXNXnotexists() throws ParseErrorException {
        assertCommandOK(array("SET", "key", "value", "EX", "1", "NX"));
        assertCommandEquals("value", array("GET", "key"));
        assertCommandNull(array("SET", "key", "value", "EX", "1", "NX"));
        assertCommandEquals(1, array("DEL", "key"));
    }

    // SET key value PX ms NX
    @Test public void testSetPXNXexpires() throws ParseErrorException, InterruptedException {
        assertCommandOK(array("SET", "key", "value", "PX", "1000", "NX"));
        assertCommandEquals("value", array("GET", "key"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "key"));
    }
    @Test public void testSetPXNXnotexists() throws ParseErrorException {
        assertCommandOK(array("SET", "key", "value", "PX", "1000", "NX"));
        assertCommandEquals("value", array("GET", "key"));
        assertCommandNull(array("SET", "key", "value", "PX", "1000", "NX"));
        assertCommandEquals(1, array("DEL", "key"));
    }

    // SET key value EX s XX
    @Test public void testSetEXXXexpires() throws ParseErrorException, InterruptedException {
        set("key", "value");
        assertCommandOK(array("SET", "key", "value", "EX", "1", "XX"));
        assertCommandEquals("value", array("GET", "key"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "key"));
    }
    @Test public void testSetEXXXnotexists() throws ParseErrorException {
        del("key");
        assertCommandNull(array("SET", "key", "value", "EX", "1", "XX"));
        assertCommandOK(array("SET", "key", "value"));
        assertCommandOK(array("SET", "key", "value", "EX", "1", "XX"));
        assertCommandEquals(1, array("DEL", "key"));
    }

    // SET key value PX ms XX
    @Test public void testSetPXXXexpires() throws ParseErrorException, InterruptedException {
        set("key", "value");
        assertCommandOK(array("SET", "key", "value", "PX", "1000", "XX"));
        assertCommandEquals("value", array("GET", "key"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "key"));
    }
    @Test public void testSetPXXXnotexists() throws ParseErrorException {
        del("key");
        assertCommandNull(array("SET", "key", "value", "PX", "1000", "XX"));
        assertCommandOK(array("SET", "key", "value"));
        assertCommandOK(array("SET", "key", "value", "PX", "1000", "XX"));
        assertCommandEquals(1, array("DEL", "key"));
    }

    @Test
    public void testUnknownCommand() throws ParseErrorException {
        assertCommandError(array("unknown"));
    }

    @Test
    public void testExpire() throws ParseErrorException, InterruptedException {
        assertCommandEquals(0, array("expire", "ab", "1"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals(1, array("expire", "ab", "1"));
        assertCommandEquals("abd", array("GET", "ab"));
        assertCommandError(array("expire", "ab", "a"));
        Thread.sleep(1000);
        assertCommandNull(array("GET", "ab"));
    }

    @Test
    public void testTTL() throws ParseErrorException, InterruptedException {
        assertCommandEquals(-2, array("ttl", "ab"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals(-1, array("ttl", "ab"));
        assertCommandEquals(1, array("expire", "ab", "2"));
        assertCommandEquals(2, array("ttl", "ab"));
        Thread.sleep(1000);
        assertCommandEquals(1, array("ttl", "ab"));
        Thread.sleep(1000);
        assertCommandEquals(-2, array("ttl", "ab"));
    }

    @Test
    public void testPTTL() throws ParseErrorException, InterruptedException {
        assertCommandEquals(-2, array("pttl", "ab"));
        assertCommandOK(array("SET", "ab", "abd"));
        assertCommandEquals(-1, array("pttl", "ab"));
        assertCommandEquals(1, array("expire", "ab", "2"));
        assertTrue(executor.execCommand(RedisCommandParser.parse(array("pttl", "ab"))).compareTo(Response.integer(1900L)) > 0);
        Thread.sleep(1100);
        assertTrue(executor.execCommand(RedisCommandParser.parse(array("pttl", "ab"))).compareTo(Response.integer(999L)) < 0);
        Thread.sleep(1000);
        assertCommandEquals(-2, array("pttl", "ab"));
    }

    @Test
    public void testIncr() throws ParseErrorException {
        assertCommandEquals(1, array("incr", "a"));
        assertCommandEquals(2, array("incr", "a"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("incr", "a"));
    }

    @Test
    public void testIncrBy() throws ParseErrorException {
        assertCommandEquals(5, array("incrby", "a", "5"));
        assertCommandEquals(11, array("incrby", "a", "6"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("incrby", "a", "1"));
        assertCommandOK(array("set", "a", "3.14"));
        assertCommandError(array("incrby", "a", "1"));
    }

    @Test
    public void testIncrByFloat() throws ParseErrorException {
        assertCommandEquals("5", array("incrbyfloat", "a", "5"));
        assertCommandEquals("11.01", array("incrbyfloat", "a", "6.01"));
        assertCommandEquals("9.51", array("incrbyfloat", "a", "-1.5"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("incrbyfloat", "a", "1"));
    }

    @Test
    public void testDecr() throws ParseErrorException {
        assertCommandEquals(-1, array("decr", "a"));
        assertCommandEquals(-2, array("decr", "a"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("decr", "a"));
    }

    @Test
    public void testDecrBy() throws ParseErrorException {
        assertCommandEquals(-5, array("decrby", "a", "5"));
        assertCommandEquals(-11, array("decrby", "a", "6"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("decrby", "a", "1"));
    }

    @Test
    public void testHll() throws ParseErrorException {
        assertCommandEquals(1, array("pfadd", "a", "b", "c"));
        assertCommandEquals(0, array("pfadd", "a", "b", "c"));
        assertCommandEquals(2, array("pfcount", "a"));
        assertCommandEquals(0, array("pfcount", "b"));
        assertCommandEquals(1, array("pfadd", "b", "c", "d"));
        assertCommandEquals(3, array("pfcount", "a", "b"));
        assertCommandOK(array("pfmerge", "c"));
        assertCommandEquals(0, array("pfcount", "c"));
        assertCommandOK(array("pfmerge", "a", "b"));
        assertCommandEquals(3, array("pfcount", "a"));
        assertCommandOK(array("set", "a", "b"));
        assertCommandError(array("pfcount", "a"));
        assertCommandError(array("pfmerge", "a"));
        assertCommandError(array("pfmerge", "b", "a"));
        assertCommandError(array("pfadd", "a", "b"));
    }

    @Test
    public void testAppend() throws ParseErrorException {
        assertCommandEquals(3, array("append", "ab", "abc"));
        assertCommandEquals(6, array("append", "ab", "abc"));
        assertCommandEquals("abcabc", array("GET", "ab"));
    }

    @Test
    public void testSetAndGetBit() throws ParseErrorException {
        assertCommandEquals(0, array("getbit", "mykey", "7"));
        assertCommandEquals(0, array("setbit", "mykey", "7", "1"));
        assertCommandEquals(1, array("getbit", "mykey", "7"));
        assertCommandEquals(0, array("getbit", "mykey", "6"));
        assertCommandEquals(1, array("setbit", "mykey", "7", "0"));
        assertEquals(Response.bulkString(Slice.create(new byte[]{0})),
                executor.execCommand(RedisCommandParser.parse(array("get", "mykey"))));
        assertCommandEquals(0, array("setbit", "mykey", "33", "1"));
        assertEquals(Response.bulkString(Slice.create(new byte[]{0, 0, 0, 0, 2})),
                executor.execCommand(RedisCommandParser.parse(array("get", "mykey"))));
        assertCommandEquals(0, array("setbit", "mykey", "22", "1"));
        assertCommandEquals(0, array("getbit", "mykey", "117"));
        assertCommandError(array("getbit", "mykey", "a"));
        assertCommandError(array("getbit", "mykey"));
        assertCommandError(array("setbit", "mykey", "a", "1"));
        assertCommandError(array("setbit", "mykey", "1"));
        assertCommandError(array("setbit", "mykey", "1", "a"));
        assertCommandError(array("setbit", "mykey", "1", "2"));
    }

    @Test
    public void testSetex() throws ParseErrorException {
        assertCommandOK(array("SETex", "ab", "100", "k"));
        assertCommandEquals(100, array("ttl", "ab"));
        assertCommandError(array("SETex", "ab", "10a", "k"));
    }

    @Test
    public void testPsetex() throws ParseErrorException {
        assertCommandOK(array("pSETex", "ab", "99", "k"));
        assertTrue(executor.execCommand(RedisCommandParser.parse(array("pttl", "ab"))).compareTo(Response.integer(90)) > 0);
        assertTrue(executor.execCommand(RedisCommandParser.parse(array("pttl", "ab"))).compareTo(Response.integer(99)) <= 0);
        assertCommandError(array("pSETex", "ab", "10a", "k"));
    }

    @Test
    public void testSetnx() throws ParseErrorException {
        assertCommandEquals(1, array("setnx", "k", "vvv"));
        assertCommandEquals("vvv", array("get", "k"));
        assertCommandEquals(0, array("setnx", "k", "ggg"));
        assertCommandEquals("vvv", array("get", "k"));
    }

    @Test
    public void testMset() throws ParseErrorException {
        assertCommandOK(array("mset", "k1", "a", "k2", "b"));
        assertCommandEquals("a", array("GET", "k1"));
        assertCommandEquals("b", array("GET", "k2"));
        assertCommandError(array("mset", "k1", "a", "k2"));
    }

    @Test
    public void testMget() throws ParseErrorException {
        assertCommandOK(array("SET", "a", "abc"));
        assertCommandOK(array("SET", "b", "abd"));

        assertEquals(array("abc", "abd", null),
                executor.execCommand(RedisCommandParser.parse(array("mget", "a", "b", "c"))).toString());
    }

    @Test
    public void testGetset() throws ParseErrorException {
        assertCommandNull(array("getSET", "a", "abc"));
        assertCommandEquals("abc", array("getSET", "a", "abd"));
    }

    @Test
    public void testHmsetAndHmget() {
        assertCommandOK(array("hmset", "h", "a", "v1", "b", "v2"));
        assertCommandArrayEquals(array("v1"), array("hmget", "h", "a"));
        assertCommandArrayEquals(array("v2"), array("hmget", "h", "b"));
    }

    @Test
    public void testDelHash() {
        assertCommandOK(array("hmset","h", "a", "v1", "b", "v2"));
        assertCommandEquals(1, array("del", "h"));
        assertCommandArrayEquals(nullArray(), array("hmget", "h", "a"));
    }

    @Test
    public void testHset() {
        assertCommandEquals(2, array("hset", "h", "a", "v1", "b", "v2"));
        assertCommandEquals(1, array("hset", "h", "a", "v1", "c", "v3"));
        assertCommandArrayEquals(array("v1"), array("hmget", "h", "a"));
        assertCommandArrayEquals(array("v2"), array("hmget", "h", "b"));
        assertCommandArrayEquals(array("v3"), array("hmget", "h", "c"));
    }

    @Test
    public void testStrlen() throws ParseErrorException {
        assertCommandEquals(0, array("strlen", "a"));
        assertCommandOK(array("SET", "a", "abd"));
        assertCommandEquals(3, array("strlen", "a"));
    }

    @Test
    public void testDel() throws ParseErrorException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandOK(array("set", "b", "v"));
        assertCommandEquals(2, array("del", "a", "b", "c"));
        assertCommandNull(array("get", "a"));
        assertCommandNull(array("get", "b"));
    }

    @Test
    public void testExists() throws ParseErrorException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("exists", "a"));
        assertCommandEquals(0, array("exists", "b"));
    }

    @Test
    public void testExpireAt() throws ParseErrorException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("expireat", "a", "1293840000"));
        assertCommandEquals(0, array("exists", "a"));
        assertCommandOK(array("set", "a", "v"));
        long now = System.currentTimeMillis() / 1000 + 5;
        assertCommandEquals(1, array("expireat", "a", String.valueOf(now)));
        assertCommandEquals(5, array("ttl", "a"));
        assertCommandError(array("expireat", "a", "a"));
    }

    @Test
    public void testPexpireAt() throws ParseErrorException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("pexpireat", "a", "1293840000000"));
        assertCommandEquals(0, array("exists", "a"));
        assertCommandEquals(0, array("pexpireat", "a", "1293840000000"));
        assertCommandOK(array("set", "a", "v"));
        long now = System.currentTimeMillis() + 5000;
        assertCommandEquals(1, array("pexpireat", "a", String.valueOf(now)));
        assertCommandEquals(5, array("ttl", "a"));
        assertCommandError(array("pexpireat", "a", "a"));
    }

    @Test
    public void testPexpire() throws ParseErrorException {
        assertCommandOK(array("set", "a", "v"));
        assertCommandEquals(1, array("pexpire", "a", "1500000"));
        assertCommandEquals(1500, array("ttl", "a"));
    }

    @Test
    public void testLpush() throws ParseErrorException {
        assertCommandEquals(1, array("lpush", "mylist", "!"));
        assertCommandEquals(3, array("lpush", "mylist", "world", "hello"));
        assertEquals(array("hello", "world", "!"),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "0", "-1"))).toString());
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("lpush", "a", "1"));
    }

    @Test
    public void testLrange() throws ParseErrorException {
        assertEquals(array(),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "0", "-1"))).toString());
        assertCommandEquals(3, array("lpush", "mylist", "1", "2", "3"));
        assertEquals(array("3", "2", "1"),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "0", "-1"))).toString());
        assertEquals(array("3", "2", "1"),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "-10", "10"))).toString());
        assertEquals(array("2"),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "1", "-2"))).toString());
        assertEquals(array(),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "10", "-10"))).toString());
        assertCommandError(array("lrange", "mylist", "a", "-1"));
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("lrange", "a", "0", "-1"));
    }

    @Test
    public void testLlen() throws ParseErrorException {
        assertCommandEquals(0, array("llen", "a"));
        assertCommandEquals(3, array("lpush", "mylist", "3", "2", "1"));
        assertCommandEquals(3, array("llen", "mylist"));
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("llen", "a"));
    }

    @Test
    public void testLpushx() throws ParseErrorException {
        assertCommandEquals(1, array("lpush", "a", "1"));
        assertCommandEquals(2, array("lpushx", "a", "2"));
        assertEquals(array("2", "1"),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "a", "0", "-1"))).toString());
        assertCommandEquals(0, array("lpushx", "b", "1"));
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("lpushx", "a", "1"));
    }

    @Test
    public void testLpop() throws ParseErrorException {
        assertCommandEquals(2, array("lpush", "list", "2", "1"));
        assertCommandEquals("1", array("lpop", "list"));
        assertCommandEquals("2", array("lpop", "list"));
        assertCommandNull(array("lpop", "list"));
        assertCommandNull(array("lpop", "notexist"));
        assertCommandOK(array("set", "key", "value"));
        assertCommandError(array("lpop", "key"));
    }

    @Test
    public void testLindex() throws ParseErrorException {
        assertCommandEquals(2, array("lpush", "list", "1", "2"));
        assertCommandEquals("2", array("lindex", "list", "0"));
        assertCommandEquals("1", array("lindex", "list", "-1"));
        assertCommandNull(array("lindex", "list", "2"));
        assertCommandNull(array("lindex", "list", "-3"));
        assertCommandError(array("lindex", "list", "a"));
        assertCommandNull(array("lindex", "notexist", "0"));
        assertCommandOK(array("set", "key", "value"));
        assertCommandError(array("lindex", "key", "1"));
    }

    @Test
    public void testRpush() throws ParseErrorException {
        assertCommandEquals(1, array("rpush", "mylist", "!"));
        assertCommandEquals(3, array("rpush", "mylist", "world", "hello"));
        assertEquals(array("!", "world", "hello"),
                executor.execCommand(RedisCommandParser.parse(array("lrange", "mylist", "0", "-1"))).toString());
        assertCommandOK(array("set", "a", "v"));
        assertCommandError(array("rpush", "a", "1"));
    }

    @Test
    public void testRename() {
        assertCommandError(array("rename", "old", "new"));
        assertCommandNull(array("get", "new"));
        assertCommandOK(array("set", "old", "abc"));
        assertCommandOK(array("rename", "old", "new"));
        assertCommandEquals("abc", array("get", "new"));
        assertCommandNull(array("get", "old"));
    }

    @Test
    public void testRenameHash() {
        assertCommandEquals(1, array("hset", "old", "a", "1"));
        assertCommandOK(array("rename", "old", "new"));
        assertCommandEquals("1", array("hget", "new", "a"));
        assertCommandNull(array("get", "old"));
    }

    @Test
    public void testRenameHashWithExistingValues() {
        assertCommandEquals(1, array("hset", "old", "a", "1"));
        assertCommandEquals(1, array("hset", "new", "z", "2"));
        assertCommandOK(array("rename", "old", "new"));
        assertCommandEquals("1", array("hget", "new", "a"));
        assertCommandNull(array("hget", "new", "z"));
        assertCommandNull(array("get", "old"));
    }

}
