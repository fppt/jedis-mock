package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class EvalTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void evalTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 'Hello, scripting!'", 0);
        assertEquals(String.class, eval_return.getClass());
        assertEquals("Hello, scripting!", eval_return);
    }

    @TestTemplate
    public void evalParametrizedTest(Jedis jedis) {
        Object eval_return = jedis.eval("return ARGV[1]", 0, "Hello");
        assertEquals(String.class, eval_return.getClass());
        assertEquals("Hello", eval_return);
    }

    @TestTemplate
    public void evalIntTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 0", 0);
        assertEquals(Long.class, eval_return.getClass());
        assertEquals(0L, eval_return);
    }

    @TestTemplate
    public void evalLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 1.123", 0);
        assertEquals(Long.class, eval_return.getClass());
        assertEquals(1L, eval_return);
    }

    @TestTemplate
    public void evalTableOfStringsTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test' }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Collections.singletonList("test"), eval_return);
    }

    @TestTemplate
    public void evalTableOfLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 1, 2, 3 }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Long.class, ((List<?>) eval_return).get(0).getClass());
        assertEquals(Arrays.asList(1L, 2L, 3L), eval_return);
    }

    @TestTemplate
    public void evalDeepListTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test', 2, {'test', 2} }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(String.class, ((List<?>) eval_return).get(0).getClass());
        assertEquals(Long.class, ((List<?>) eval_return).get(1).getClass());
        assertEquals(ArrayList.class, ((List<?>) eval_return).get(2).getClass());
        assertEquals(Arrays.asList("test", 2L, Arrays.asList("test", 2L)), eval_return);
    }

    @TestTemplate
    public void evalDictTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { a = 1, 2 }", 0);
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Long.class, ((List<?>) eval_return).get(0).getClass());
        assertEquals(Collections.singletonList(2L), eval_return);
    }

    @TestTemplate
    public void stringConversion(Jedis jedis) {
        String script = "return {ok='fine'}";
        assertEquals("fine", jedis.eval(script, 0));
    }

    @TestTemplate
    public void evalParametrizedReturnMultipleKeysArgsTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }",
                2, "key1", "key2",
                "arg1", "arg2", "arg3"
        );
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Arrays.asList("key1", "key2", "arg1", "arg2", "arg3"), eval_return);
    }

    @TestTemplate
    public void evalParametrizedReturnMultipleKeysArgsNumbersTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], tonumber(ARGV[1]) }",
                2, "key1", "key2",
                "1"
        );
        assertEquals(ArrayList.class, eval_return.getClass());
        assertEquals(Arrays.asList("key1", "key2", 1L), eval_return);
    }

    @TestTemplate
    public void evalRedisSetTest(Jedis jedis) {
        assertEquals("OK", jedis.eval("return redis.call('SET', 'test', 'hello')", 0));
        assertEquals("hello", jedis.get("test"));
    }

    @TestTemplate
    public void evalRedisDecrTest(Jedis jedis) {
        jedis.eval("redis.call('SET', 'count', '1')", 0);
        assertEquals(0L, jedis.eval("return redis.call('DECR', 'count')", 0));
    }

    @TestTemplate
    public void evalRedisRecursiveTest(Jedis jedis) {
        Exception e = assertThrows(RuntimeException.class, () -> jedis.eval("return redis.call('EVAL', 'return { 1, 2, 3 }', '0')", 0));
        assertNotNull(e);
    }

    @TestTemplate
    public void evalRedisReturnPcallResultsInExceptionTest(Jedis jedis) {
        JedisDataException e = assertThrows(JedisDataException.class, () -> jedis.eval("return redis.pcall('RENAME','A','B')", 0));
        assertNotNull(e);
    }

    @TestTemplate
    public void evalRedisPCallCanHandleExceptionTest(Jedis jedis) {
        assertEquals("Handled error from pcall", jedis.eval("" +
                        "local reply = redis.pcall('RENAME','A','B')\n" +
                        "if reply['err'] ~= nil then\n" +
                        "  return 'Handled error from pcall'" +
                        "end\n" +
                        "return reply",
                0));
    }

    @TestTemplate
    public void evalRedisPCallDoesNotThrowTest(Jedis jedis) {
        assertNull(jedis.eval("redis.pcall('RENAME','A','B')", 0));
    }

    @TestTemplate
    public void fibonacciScript(Jedis jedis) {
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
    public void trailingComment(Jedis jedis) {
        assertEquals("hello", jedis.eval("return 'hello' --trailing comment", 0));
    }

    @TestTemplate
    public void manyArgumentsTest(Jedis jedis) {
        String script = "return redis.call('SADD', 'myset', 1, 2, 3, 4, 5, 6)";
        jedis.eval(script, 0);
        assertEquals(6, jedis.scard("myset"));
    }

    @TestTemplate
    public void booleanTrueConversion(Jedis jedis) {
        String script = "return true";
        assertEquals(1L, jedis.eval(script, 0));
    }

    @TestTemplate
    public void booleanFalseConversion(Jedis jedis) {
        String script = "return false";
        assertNull(jedis.eval(script, 0));
    }

    @TestTemplate
    public void sha1hexImplementation(Jedis jedis){
        String script = "return redis.sha1hex('Pizza & Mandolino')";
        assertEquals("74822d82031af7493c20eefa13bd07ec4fada82f",
                jedis.eval(script, 0));
    }
}
