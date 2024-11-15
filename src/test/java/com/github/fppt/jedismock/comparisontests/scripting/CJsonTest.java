package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Collections;

import static java.util.Arrays.asList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
class CJsonTest {

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
    void evalCjsonEncodeJustNilTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode(nil)"))
                .isEqualTo("null");
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
    void evalCjsonEncodeNestedJsonTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = {['bar'] = 'baz'}})"))
                .isEqualTo("{\"foo\":{\"bar\":\"baz\"}}");
    }

    @TestTemplate
    void evalCjsonEncodeNestedJsonArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = {'bar', 'baz'}})"))
                .isEqualTo("{\"foo\":[\"bar\",\"baz\"]}");
    }

    @TestTemplate
    void evalCjsonEncodeJsonArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({{['foo'] = 'bar'}})"))
                .isEqualTo("[{\"foo\":\"bar\"}]");
    }

    @TestTemplate
    void evalCjsonEncodeJsonNullTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({['foo'] = nil})"))
                .isEqualTo("{}");
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
    void evalCjsonDecodeJustQuotedStringTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('\"str\"')"))
                .isEqualTo("str");
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
    void evalCjsonDecodeJsonArrayTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('[{\"foo\":[{\"bar\":\"baz\"}]}]')[1]['foo'][1]['bar']"))
                .isEqualTo("baz");
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
    void evalCjsonDecodeJustStringTest(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("return cjson.decode('invalid')"))
                .isInstanceOf(JedisDataException.class);
    }

    @TestTemplate
    void evalCjsonDecodeInvalidJsonTest(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("return cjson.decode('{\"foo\":\"bar\"')"))
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

    @TestTemplate
    void evalCjsonDecodeNumeric(Jedis jedis) {
        String script = "return table.concat(\n"
                + "cjson.decode(\n"
                + "\"[0.0, -5e3, -1, 1023.2, 0e10]\"), \" \")";
        assertThat(jedis.eval(script)).isEqualTo("0 -5000 -1 1023.2 0");
    }

    @TestTemplate
    void evalCjsonSmokeMapTest(Jedis jedis) {
        String string = "local some_map = {\n"
                + "    s1 = \"Some string\",\n"
                + "    n1 = 100,\n"
                + "    n2 = 100.01,\n"
                + "    a1 = { \"Some\", \"String\", \"Array\" },\n"
                + "    nil1 = nil,\n"
                + "    b1 = true,\n"
                + "    b2 = false\n"
                + "}\n"
                + "local encoded = cjson.encode(some_map)\n"
                + "local decoded = cjson.decode(encoded)\n"
                + "return table.concat(some_map) == table.concat(decoded)\n";
        assertThat(jedis.eval(string)).isEqualTo(1L);
    }

    @TestTemplate
    void evalCjsonEncodeInvalidKeyBooleanTest(Jedis jedis) {
        String script = "local invalid_map = {}\n"
                + "invalid_map[false] = 'false'\n"
                + "local ok, _ = pcall(cjson.encode, invalid_map)\n"
                + "return ok == false";
        assertThat(jedis.eval(script)).isEqualTo(1L);
    }

    @TestTemplate
    void evalCjsonEncodeInvalidKeyTableTest(Jedis jedis) {
        String script = "local invalid_map = {}\n"
                + "invalid_map[{}] = 'table'\n"
                + "local ok, _ = pcall(cjson.encode, invalid_map)\n"
                + "return ok == false";
        assertThat(jedis.eval(script)).isEqualTo(1L);
    }

    @TestTemplate
    void evalCjsonEncodeDocsExampleTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.encode({ ['foo'] = 'bar' })"))
                .isEqualTo("{\"foo\":\"bar\"}");
    }

    @TestTemplate
    void evalCjsonDecodeDocsExampleTest(Jedis jedis) {
        assertThat(jedis.eval("return cjson.decode('{\"foo\":\"bar\"}')['foo']"))
                .isEqualTo("bar");
    }

    // unsupported cjson features
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
    @Disabled("Lua treats nil values in array as end of array. Current mock result = [1]")
    void evalCjsonDecodeJsonArrayWithNilTest(Jedis jedis) {
        String script = "local decoded = cjson.decode('{\"foo\":[1, null, 3]}')\n"
                + "return {decoded['foo'][1], decoded['foo'][2], decoded['foo'][3]}";
        assertThat(jedis.eval(script))
                .isEqualTo(asList(1L, null, 3L));
    }

    @TestTemplate
    @Disabled("JedisMock doesn't convert scientific notation numbers to decimal. Current mock result = 3.0E-4")
    void evalCjsonDecodeScientificNotationNumber(Jedis jedis) {
        String script = "return table.concat(cjson.decode('[3.0e-4]'), ' ')";
        assertThat(jedis.eval(script)).isEqualTo("0.0003");
    }

    @TestTemplate
    @Disabled("Cjson in JedisMock is not represented as a read-only table, so user can modify it")
    void evalCjsonIsReadOnlyTable(Jedis jedis) {
        assertThatThrownBy(() -> jedis.eval("cjson.encode = function() return 1 end"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageContaining("Attempt to modify a readonly table");
    }
}
