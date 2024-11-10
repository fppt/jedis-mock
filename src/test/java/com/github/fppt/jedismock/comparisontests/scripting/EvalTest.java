package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

@ExtendWith(ComparisonBase.class)
public class EvalTest {
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void evalTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 'Hello, scripting!'", 0);
        assertThat(eval_return).isInstanceOf(String.class).isEqualTo("Hello, scripting!");
    }

    @TestTemplate
    void evalParametrizedTest(Jedis jedis) {
        Object eval_return = jedis.eval("return ARGV[1]", 0, "Hello");
        assertThat(eval_return).isInstanceOf(String.class).isEqualTo("Hello");
    }

    @TestTemplate
    void evalIntTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 0", 0);
        assertThat(eval_return).isInstanceOf(Long.class).isEqualTo(0L);
    }

    @TestTemplate
    void evalLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return 1.123", 0);
        assertThat(eval_return).isInstanceOf(Long.class).isEqualTo(1L);
    }

    @TestTemplate
    void evalTableOfStringsTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test' }", 0);
        assertThat(eval_return).isInstanceOf(ArrayList.class).isEqualTo(singletonList("test"));
    }

    @TestTemplate
    void evalTableOfLongTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 1, 2, 3 }", 0);
        assertThat(eval_return).isInstanceOf(ArrayList.class)
                .asInstanceOf(LIST).containsExactly(1L, 2L, 3L);
        assertThat(((List<?>) eval_return).get(0)).isInstanceOf(Long.class);
    }

    @TestTemplate
    void evalDeepListTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { 'test', 2, {'test', 2} }", 0);
        assertThat(eval_return).isInstanceOf(ArrayList.class)
                .asInstanceOf(LIST).containsExactly("test", 2L, asList("test", 2L));
        assertThat(((List<?>) eval_return).get(0)).isInstanceOf(String.class);
        assertThat(((List<?>) eval_return).get(1)).isInstanceOf(Long.class);
        assertThat(((List<?>) eval_return).get(2)).isInstanceOf(ArrayList.class);
    }

    @TestTemplate
    void evalDictTest(Jedis jedis) {
        Object eval_return = jedis.eval("return { a = 1, 2 }", 0);
        assertThat(eval_return).isInstanceOf(ArrayList.class)
                .asInstanceOf(LIST).containsExactly(2L);
        assertThat(((List<?>) eval_return).get(0)).isInstanceOf(Long.class);
    }

    @TestTemplate
    void okFieldConversion(Jedis jedis) {
        String script = "return {ok='fine'}";
        assertThat(jedis.eval(script, 0)).isEqualTo("fine");
    }

    @TestTemplate
    void errFieldConversion(Jedis jedis) {
        String script = "return {err='bad'}";
        assertThatThrownBy(() -> jedis.eval(script, 0))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("bad");
    }

    @TestTemplate
    void statusReplyAPI(Jedis jedis) {
        String script = "return redis.status_reply('Everything is fine')";
        assertThat(jedis.eval(script, 0)).isEqualTo("Everything is fine");
    }

    @TestTemplate
    void errorReplyAPI(Jedis jedis) {
        String script = "return redis.error_reply('Something bad happened')";
        assertThatThrownBy(() -> jedis.eval(script, 0))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("Something bad happened");
    }

    @TestTemplate
    void logLevelsAPI(Jedis jedis) {
        assertThat(jedis.eval("return redis.LOG_DEBUG")).isEqualTo(0L);
        assertThat(jedis.eval("return redis.LOG_VERBOSE")).isEqualTo(1L);
        assertThat(jedis.eval("return redis.LOG_NOTICE")).isEqualTo(2L);
        assertThat(jedis.eval("return redis.LOG_WARNING")).isEqualTo(3L);
    }

    @TestTemplate
    void logAPI(Jedis jedis) {
        assertThat(jedis.eval("return redis.log(redis.LOG_DEBUG, 'Something is happening')")).isNull();
        assertThat(jedis.eval("return redis.log(redis.LOG_VERBOSE, 'Blah-blah')")).isNull();
        assertThat(jedis.eval("return redis.log(redis.LOG_NOTICE, 'Notice this')")).isNull();
        assertThat(jedis.eval("return redis.log(redis.LOG_WARNING, 'Something is wrong')")).isNull();
    }

    @TestTemplate
    void evalParametrizedReturnMultipleKeysArgsTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }",
                2, "key1", "key2",
                "arg1", "arg2", "arg3"
        );
        assertThat(eval_return).isInstanceOf(ArrayList.class).isEqualTo(asList("key1", "key2", "arg1", "arg2", "arg3"));
    }

    @TestTemplate
    void evalParametrizedReturnMultipleKeysArgsNumbersTest(Jedis jedis) {
        Object eval_return = jedis.eval(
                "return { KEYS[1], KEYS[2], tonumber(ARGV[1]) }",
                2, "key1", "key2",
                "1"
        );
        assertThat(eval_return).isInstanceOf(ArrayList.class).isEqualTo(asList("key1", "key2", 1L));
    }

    @TestTemplate
    void evalRedisSetTest(Jedis jedis) {
        assertThat(jedis.eval("return redis.call('SET', 'test', 'hello')", 0)).isEqualTo("OK");
        assertThat(jedis.get("test")).isEqualTo("hello");
    }

    @TestTemplate
    void evalRedisDecrTest(Jedis jedis) {
        jedis.eval("redis.call('SET', 'count', '1')", 0);
        assertThat(jedis.eval("return redis.call('DECR', 'count')", 0)).isEqualTo(0L);
    }

    @TestTemplate
    void evalRedisRecursiveTest(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("return redis.call('EVAL', 'return { 1, 2, 3 }', '0')", 0))
                .isInstanceOf(RuntimeException.class)
                .isNotNull();
    }

    @TestTemplate
    void evalRedisReturnPcallResultsInExceptionTest(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("return redis.pcall('RENAME','A','B')", 0))
                .isInstanceOf(JedisDataException.class)
                .isNotNull();
    }

    @TestTemplate
    void evalRedisPCallCanHandleExceptionTest(Jedis jedis) {
        assertThat(jedis.eval(
                "local reply = redis.pcall('RENAME','A','B')\n" +
                        "if reply['err'] ~= nil then\n" +
                        "  return 'Handled error from pcall'" +
                        "end\n" +
                        "return reply",
                0)).isEqualTo("Handled error from pcall");
    }

    @TestTemplate
    void evalRedisPCallDoesNotThrowTest(Jedis jedis) {
        assertThat(jedis.eval("redis.pcall('RENAME','A','B')", 0)).isNull();
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
        assertThat(jedis.lrange("mylist", 0, -1)).containsExactly("1", "2", "3", "5", "8", "13", "21", "34", "55");
    }

    @TestTemplate
    void trailingComment(Jedis jedis) {
        assertThat(jedis.eval("return 'hello' --trailing comment", 0)).isEqualTo("hello");
    }

    @TestTemplate
    void manyArgumentsTest(Jedis jedis) {
        String script = "return redis.call('SADD', 'myset', 1, 2, 3, 4, 5, 6)";
        jedis.eval(script, 0);
        assertThat(jedis.scard("myset")).isEqualTo(6);
    }

    @TestTemplate
    void booleanTrueConversion(Jedis jedis) {
        String script = "return true";
        assertThat(jedis.eval(script, 0)).isEqualTo(1L);
    }

    @TestTemplate
    void booleanFalseConversion(Jedis jedis) {
        String script = "return false";
        assertThat(jedis.eval(script, 0)).isNull();
    }

    @TestTemplate
    void sha1hexImplementation(Jedis jedis) {
        String script = "return redis.sha1hex('Pizza & Mandolino')";
        assertThat(jedis.eval(script, 0)).isEqualTo("74822d82031af7493c20eefa13bd07ec4fada82f");
    }

    @TestTemplate
    void selectUsesSelectedDB(Jedis jedis) {
        jedis.select(5);
        jedis.set("foo", "DB5");
        jedis.select(6);
        jedis.set("foo", "DB6");
        jedis.select(5);
        assertThat(jedis.eval("return redis.call('get', 'foo')")).isEqualTo("DB5");
    }

    @TestTemplate
    void luaSelectDoesNotAffectSelectedDB(Jedis jedis) {
        jedis.select(5);
        jedis.set("foo", "DB5");
        jedis.select(6);
        jedis.set("foo", "DB6");
        assertThat(jedis.eval("redis.call('select', 5); return redis.call('get', 'foo')")).isEqualTo("DB5");
        assertThat(jedis.get("foo")).isEqualTo("DB6");
    }

    @TestTemplate
    public void luaReturnsNullFromEmptyMap(Jedis jedis) {
        String s = "return redis.call('hget', KEYS[1], ARGV[1])";
        Object res = jedis.eval(s, Collections.singletonList("foo"),
                Collections.singletonList("bar"));
        assertThat(res).isNull();
    }

    @TestTemplate
    public void luaReturnsNullFromNonExistentKey(Jedis jedis) {
        String s = "return redis.call('get', KEYS[1])";
        Object res = jedis.eval(s, Collections.singletonList("foo"),
                Collections.emptyList());
        assertThat(res).isNull();
    }

    @TestTemplate
    public void callReturnsFalseFromNonExistingKey(Jedis jedis) {
        Object result = jedis.eval(
                "local val = redis.call('get', KEYS[1]); return val ~= false; ",
                Collections.singletonList("an-example-key"),
                Collections.emptyList()
        );
        assertThat(result).isNull();
    }

    @TestTemplate
    public void callReturnsNonFalseForExistingKey(Jedis jedis) {
        jedis.set("an-example-key", "17");
        Object result = jedis.eval("local val = redis.call('get', KEYS[1]); return val ~= false; ",
                Collections.singletonList("an-example-key"),
                Collections.emptyList()
        );
        assertThat(result).isEqualTo(1L);
    }

    @TestTemplate
    public void evalUnpackTest(Jedis jedis) {
        jedis.eval("redis.call(\"SADD\", \"myset\", unpack({ \"the\", \"quick\", \"brown\", \"fox\" }))");
        assertThat(jedis.smembers("myset"))
                .containsExactly("the", "quick", "brown", "fox");
    }

    @TestTemplate
    void evalCjsonEncodeJustBooleanTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode(true)"))
                .isEqualTo(Boolean.TRUE.toString());
    }

    @TestTemplate
    void evalCjsonEncodeJustIntegerTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode(100)"))
                .isEqualTo("100");
    }

    @TestTemplate
    void evalCjsonEncodeJustDoubleTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode(100.01)"))
                .isEqualTo("100.01");
    }

    @TestTemplate
    void evalCjsonEncodeJustStringTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode('str')"))
                .isEqualTo("\"str\"");
    }

    @TestTemplate
    @Disabled("luaj doesn't pass nil values in org.luaj.vm2.LuaTable: current mock result = {\"1\":1,\"3\":3}")
    void evalCjsonEncodeJustArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({1, nil, 3})"))
                .isEqualTo("[1,null,3]");
    }

    @TestTemplate
    @Disabled("LuaJ doesn't pass nil values in org.luaj.vm2.LuaTable."
            + " current mock result = {\"1\":1,\"2\":\"str\",\"3\":true,\"5\":[1,2],\"6\":{\"foo\":\"bar\"}}")
    void evalCjsonEncodeArrayWithMixedTypesTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({1, 'str', true, nil, {1, 2}, {['foo'] = 'bar'}})"))
                .isEqualTo("[1,\"str\",true,null,[1,2],{\"foo\":\"bar\"}]");
    }

    @TestTemplate
    void evalCjsonEncodeJsonBooleanTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = true})"))
                .isEqualTo("{\"foo\":true}");
    }

    @TestTemplate
    void evalCjsonEncodeJsonIntegerTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = 100})"))
                .isEqualTo("{\"foo\":100}");
    }

    @TestTemplate
    void evalCjsonEncodeJsonDoubleTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = 100.01})"))
                .isEqualTo("{\"foo\":100.01}");
    }

    @TestTemplate
    void evalCjsonEncodeJsonStringTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = 'str'})"))
                .isEqualTo("{\"foo\":\"str\"}");
    }

    @TestTemplate
    void evalCjsonEncodeNestedJsonTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = {['bar'] = 'baz'}})"))
                .isEqualTo("{\"foo\":{\"bar\":\"baz\"}}");
    }

    @TestTemplate
    void evalCjsonEncodeJsonArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = {'bar', 'baz'}})"))
                .isEqualTo("{\"foo\":[\"bar\",\"baz\"]}");
    }

    @TestTemplate
    void evalCjsonEncodeJsonArray(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({{['foo'] = 'bar'}})"))
                .isEqualTo("[{\"foo\":\"bar\"}]");
    }

    @TestTemplate
    void evalCjsonEncodeJsonNullTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = nil})"))
                .isEqualTo("{}");
    }

    @TestTemplate
    void evalCjsonEncodeNilTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode(nil)"))
                .isEqualTo("null");
    }

    @TestTemplate
    void evalCjsonEncodeEmptyTableTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({})"))
                .isEqualTo("{}");
    }

    @TestTemplate
    void evalCjsonDecodeJustBooleanTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('true')"))
                .isEqualTo(1L);
    }

    @TestTemplate
    void evalCjsonDecodeJustIntegerTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('100')"))
                .isEqualTo(100L);
    }

    @TestTemplate
    void evalCjsonDecodeJustDoubleTest(Jedis jedis) {
        assertThat(jedis.eval("return tostring(cjson.decode('100.01'))"))
                .isEqualTo("100.01");
    }

    @TestTemplate
    void evalCjsonDecodeJustStringTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('\"str\"')"))
                .isEqualTo("str");
    }

    @TestTemplate
    void evalCjsonDecodeJustArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('[1, 2, 3]')"))
                .isEqualTo(asList(1L, 2L, 3L));
    }

    @TestTemplate
    void evalCjsonDecodeJustNullTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('null')"))
                .isNull();
    }

    @TestTemplate
    void evalCjsonDecodeJsonBooleanTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":true}')['foo']"))
                .isEqualTo(1L);
    }

    @TestTemplate
    void evalCjsonDecodeJsonNullTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":null}')['foo']"))
                .isNull();
    }

    @TestTemplate
    void evalCjsonDecodeJsonIntegerTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":100}')['foo']"))
                .isEqualTo(100L);
    }

    @TestTemplate
    void evalCjsonDecodeJsonDoubleTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":100.00}')['foo']"))
                .isEqualTo(100L);
    }

    @TestTemplate
    void evalCjsonDecodeJsonStringTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":\"bar\"}')['foo']"))
                .isEqualTo("bar");
    }

    @TestTemplate
    void evalCjsonDecodeJsonArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('[{\"foo\":[{\"bar\":\"baz\"}]}]')[1]['foo'][1]['bar']"))
                .isEqualTo("baz");
    }

    @TestTemplate
    @Disabled("Lua treats nil as a end of table. Current mock result = [1L]")
    void evalCjsonDecodeJsonArrayWithNilTest(Jedis jedis) {
        String script = "local decoded = cjson.decode('{\"foo\":[1, null, 3]}')\n"
                + "return {decoded['foo'][1], decoded['foo'][2], decoded['foo'][3]}";
        assertThat(jedis.eval(script))
                .isEqualTo(asList(1L, null, 3L));
    }

    @TestTemplate
    void evalCjsonDecodeNestedJsonTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":{\"bar\":\"baz\"}}')['foo']['bar']"))
                .isEqualTo("baz");
    }

    @TestTemplate
    void evalCjsonDecodeJsonArray(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('[{\"foo\":\"bar\"}]')[1]['foo']"))
                .isEqualTo("bar");
    }

    @TestTemplate
    void evalCjsonDecodeNilTest(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("return cjson.decode(nil)"))
                .isInstanceOf(JedisDataException.class);
    }

    @TestTemplate
    void evalCjsonDecodeEmptyTableTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{}')"))
                .isEqualTo(Collections.emptyList());
    }

    @TestTemplate
    void evalCjsonDecodeEmptyArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('[]')"))
                .isEqualTo(Collections.emptyList());
    }
}
