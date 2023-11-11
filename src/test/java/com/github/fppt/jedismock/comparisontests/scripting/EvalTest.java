package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class EvalTest {
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void evalTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 'Hello, scripting!'", 0);
        assertEquals(String.class, eval_return.getClass());
        assertEquals("Hello, scripting!", eval_return);
    }

    @TestTemplate
    void evalParametrizedTest(Jedis jedis) {
        Object eval_return = jedis.eval("return ARGV[1]", 0, "Hello");
        assertEquals(String.class, eval_return.getClass());
        assertEquals("Hello", eval_return);
    }

    @TestTemplate
    void evalIntTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 0", 0);
        assertEquals(Long.class, eval_return.getClass());
        assertEquals(0L, eval_return);
    }

    @TestTemplate
    void evalLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 1.123", 0);
        assertEquals(Long.class, eval_return.getClass());
        assertEquals(1L, eval_return);
    }

    @TestTemplate
    void evalTableOfStringsTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test' }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Collections.singletonList("test"), eval_return);
    }

    @TestTemplate
    void evalTableOfLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 1, 2, 3 }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Long.class, ((List<?>) eval_return).get(0).getClass());
        assertEquals(Arrays.asList(1L, 2L, 3L), eval_return);
    }

    @TestTemplate
    void evalDeepListTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test', 2, {'test', 2} }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(String.class, ((List<?>) eval_return).get(0).getClass());
        assertEquals(Long.class, ((List<?>) eval_return).get(1).getClass());
        assertEquals(ArrayList.class, ((List<?>) eval_return).get(2).getClass());
        assertEquals(Arrays.asList("test", 2L, Arrays.asList("test", 2L)), eval_return);
    }

    @TestTemplate
    void evalDictTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { a = 1, 2 }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Long.class, ((List<?>) eval_return).get(0).getClass());
        assertEquals(Collections.singletonList(2L), eval_return);
    }

    @TestTemplate
    void okFieldConversion(Jedis jedis) {
        String script = "return {ok='fine'}";
        assertEquals("fine", jedis.eval(script, 0));
    }

    @TestTemplate
    void errFieldConversion(Jedis jedis) {
        String script = "return {err='bad'}";
        String message = assertThrows(JedisDataException.class,
                () -> jedis.eval(script, 0)).getMessage();
        assertEquals("bad", message);
    }

    @TestTemplate
    void statusReplyAPI(Jedis jedis) {
        String script = "return redis.status_reply('Everything is fine')";
        assertEquals("Everything is fine", jedis.eval(script, 0));
    }

    @TestTemplate
    void errorReplyAPI(Jedis jedis) {
        String script = "return redis.error_reply('Something bad happened')";
        String message = assertThrows(JedisDataException.class,
                () -> jedis.eval(script, 0)).getMessage();
        assertEquals("Something bad happened", message);
    }

    @TestTemplate
    void logLevelsAPI(Jedis jedis) {
        assertEquals(0L, jedis.eval("return redis.LOG_DEBUG"));
        assertEquals(1L, jedis.eval("return redis.LOG_VERBOSE"));
        assertEquals(2L, jedis.eval("return redis.LOG_NOTICE"));
        assertEquals(3L, jedis.eval("return redis.LOG_WARNING"));
    }

    @TestTemplate
    void logAPI(Jedis jedis) {
        assertNull(jedis.eval("return redis.log(redis.LOG_DEBUG, 'Something is happening')"));
        assertNull(jedis.eval("return redis.log(redis.LOG_VERBOSE, 'Blah-blah')"));
        assertNull(jedis.eval("return redis.log(redis.LOG_NOTICE, 'Notice this')"));
        assertNull(jedis.eval("return redis.log(redis.LOG_WARNING, 'Something is wrong')"));
    }

    @TestTemplate
    void evalParametrizedReturnMultipleKeysArgsTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }",
                2, "key1", "key2",
                "arg1", "arg2", "arg3"
        );
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Arrays.asList("key1", "key2", "arg1", "arg2", "arg3"), eval_return);
    }

    @TestTemplate
    void evalParametrizedReturnMultipleKeysArgsNumbersTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], tonumber(ARGV[1]) }",
                2, "key1", "key2",
                "1"
        );
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Arrays.asList("key1", "key2", 1L), eval_return);
    }

    @TestTemplate
    void evalRedisSetTest(Jedis jedis) {
        assertEquals("OK", jedis.eval("return redis.call('SET', 'test', 'hello')", 0));
        assertEquals("hello", jedis.get("test"));
    }

    @TestTemplate
    void evalRedisDecrTest(Jedis jedis) {
        jedis.eval("redis.call('SET', 'count', '1')", 0);
        assertEquals(0L, jedis.eval("return redis.call('DECR', 'count')", 0));
    }

    @TestTemplate
    void evalRedisRecursiveTest(Jedis jedis) {
        Exception e = assertThrows(RuntimeException.class, () -> jedis.eval("return redis.call('EVAL', 'return { 1, 2, 3 }', '0')", 0));
        assertNotNull(e);
    }

    @TestTemplate
    void evalRedisReturnPcallResultsInExceptionTest(Jedis jedis) {
        JedisDataException e = assertThrows(JedisDataException.class, () -> jedis.eval("return redis.pcall('RENAME','A','B')", 0));
        assertNotNull(e);
    }

    @TestTemplate
    void evalRedisPCallCanHandleExceptionTest(Jedis jedis) {
        assertEquals("Handled error from pcall", jedis.eval(
                        "local reply = redis.pcall('RENAME','A','B')\n" +
                        "if reply['err'] ~= nil then\n" +
                        "  return 'Handled error from pcall'" +
                        "end\n" +
                        "return reply",
                0));
    }

    @TestTemplate
    void evalRedisPCallDoesNotThrowTest(Jedis jedis) {
        assertNull(jedis.eval("redis.pcall('RENAME','A','B')", 0));
    }

    @TestTemplate
    void fibonacciScript(Jedis jedis) {
        String script =
                "local a, b = 0, 1\n" +
                        "for i = 2, ARGV[1] do\n" +
                        "  local temp = a + b\n" +
                        "  a = b\n" +
                        "  b = temp\n" +
                        "  redis.call('RPUSH',KEYS[1], temp)\n" +
                        "end\n";
        jedis.eval(script, 1, "mylist", "10");
        assertEquals(Arrays.asList("1", "2", "3", "5", "8", "13", "21", "34", "55"),
                jedis.lrange("mylist", 0, -1));
    }

    @TestTemplate
    void trailingComment(Jedis jedis) {
        assertEquals("hello", jedis.eval("return 'hello' --trailing comment", 0));
    }

    @TestTemplate
    void manyArgumentsTest(Jedis jedis) {
        String script = "return redis.call('SADD', 'myset', 1, 2, 3, 4, 5, 6)";
        jedis.eval(script, 0);
        assertEquals(6, jedis.scard("myset"));
    }

    @TestTemplate
    void booleanTrueConversion(Jedis jedis) {
        String script = "return true";
        assertEquals(1L, jedis.eval(script, 0));
    }

    @TestTemplate
    void booleanFalseConversion(Jedis jedis) {
        String script = "return false";
        assertNull(jedis.eval(script, 0));
    }

    @TestTemplate
    void sha1hexImplementation(Jedis jedis) {
        String script = "return redis.sha1hex('Pizza & Mandolino')";
        assertEquals("74822d82031af7493c20eefa13bd07ec4fada82f",
                jedis.eval(script, 0));
    }

    @TestTemplate
    void selectUsesSelectedDB(Jedis jedis) {
        jedis.select(5);
        jedis.set("foo", "DB5");
        jedis.select(6);
        jedis.set("foo", "DB6");
        jedis.select(5);
        assertEquals("DB5", jedis.eval("return redis.call('get', 'foo')"));
    }

    @TestTemplate
    void luaSelectDoesNotAffectSelectedDB(Jedis jedis) {
        jedis.select(5);
        jedis.set("foo", "DB5");
        jedis.select(6);
        jedis.set("foo", "DB6");
        assertEquals("DB5", jedis.eval("redis.call('select', 5); return redis.call('get', 'foo')"));
        assertEquals("DB6", jedis.get("foo"));
    }

    @TestTemplate
    public void luaReturnsNullFromEmptyMap(Jedis jedis) {
        String s = "return redis.call('hget', KEYS[1], ARGV[1])";
        Object res = jedis.eval(s, Collections.singletonList("foo"),
                Collections.singletonList("bar"));
        assertNull(res);
    }

    @TestTemplate
    public void luaReturnsNullFromNonExistentKey(Jedis jedis) {
        String s = "return redis.call('get', KEYS[1])";
        Object res = jedis.eval(s, Collections.singletonList("foo"),
                Collections.emptyList());
        assertNull(res);
    }

    @TestTemplate
    public void callReturnsFalseFromNonExistingKey(Jedis jedis) {
        Object result = jedis.eval(
                "local val = redis.call('get', KEYS[1]); return val ~= false; ",
                Collections.singletonList("an-example-key"),
                Collections.emptyList()
        );
        Assertions.assertNull(result);
    }

    @TestTemplate
    public void callReturnsNonFalseForExistingKey(Jedis jedis) {
        jedis.set("an-example-key", "17");
        Object result = jedis.eval("local val = redis.call('get', KEYS[1]); return val ~= false; ",
                Collections.singletonList("an-example-key"),
                Collections.emptyList()
        );
        Assertions.assertEquals(1L, result);
    }
}
