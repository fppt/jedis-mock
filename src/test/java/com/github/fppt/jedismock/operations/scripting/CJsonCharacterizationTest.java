package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Characterization tests for {@code cjson.encode} / {@code cjson.decode}.
 *
 * <p>{@link com.github.fppt.jedismock.comparisontests.scripting.CJsonTest} is the acceptance
 * test for cjson, but it only runs against a real Redis container. This test pins the same
 * behaviour without Docker so the JSON codec underneath can be replaced safely: every
 * expectation here was recorded against the original gson-backed implementation and must keep
 * holding afterwards, unedited.
 *
 * <p>It deliberately goes further than {@code CJsonTest} and also pins behaviour that
 * {@code CJsonTest} never looks at but that a codec rewrite could silently change: string
 * escaping, number formatting, map-key rendering and the exact deviations that
 * {@code CJsonTest} documents with {@code @Disabled}.
 *
 * <p>What is deliberately <em>not</em> pinned: gson parsed with a lenient reader, so it also
 * accepted input that is not JSON at all (unquoted names, single-quoted strings, comments,
 * {@code NaN}/{@code Infinity} literals nested inside a document). Real Redis rejects all of
 * those, {@code CJsonTest} never exercises them, and pinning them would mean re-implementing
 * gson's lenient mode; they are treated as accidents of the old implementation rather than
 * behaviour.
 */
class CJsonCharacterizationTest {
    private RedisServer server;
    private Jedis jedis;

    @BeforeEach
    void setUp() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();
        jedis = new Jedis(server.getHost(), server.getBindPort());
    }

    @AfterEach
    void tearDown() throws IOException {
        jedis.close();
        server.stop();
    }

    private Object eval(String script) {
        return jedis.eval(script);
    }

    // ---------------------------------------------------------------- encode: scalars

    @Test
    void encodesBoolean() {
        assertThat(eval("return cjson.encode(true)")).isEqualTo("true");
        assertThat(eval("return cjson.encode(false)")).isEqualTo("false");
    }

    @Test
    void encodesInteger() {
        assertThat(eval("return cjson.encode(100)")).isEqualTo("100");
    }

    @Test
    void encodesDouble() {
        assertThat(eval("return cjson.encode(100.01)")).isEqualTo("100.01");
    }

    @Test
    void encodesString() {
        assertThat(eval("return cjson.encode('str')")).isEqualTo("\"str\"");
    }

    @Test
    void encodesNil() {
        assertThat(eval("return cjson.encode(nil)")).isEqualTo("null");
    }

    // ---------------------------------------------------------------- encode: tables

    @Test
    void encodesTableWithBooleanValue() {
        assertThat(eval("return cjson.encode({['foo'] = true})")).isEqualTo("{\"foo\":true}");
    }

    @Test
    void encodesTableWithIntegerValue() {
        assertThat(eval("return cjson.encode({['foo'] = 100})")).isEqualTo("{\"foo\":100}");
    }

    @Test
    void encodesTableWithDoubleValue() {
        assertThat(eval("return cjson.encode({['foo'] = 100.01})")).isEqualTo("{\"foo\":100.01}");
    }

    @Test
    void encodesNestedTable() {
        assertThat(eval("return cjson.encode({['foo'] = {['bar'] = 'baz'}})"))
                .isEqualTo("{\"foo\":{\"bar\":\"baz\"}}");
    }

    @Test
    void encodesNestedArray() {
        assertThat(eval("return cjson.encode({['foo'] = {'bar', 'baz'}})"))
                .isEqualTo("{\"foo\":[\"bar\",\"baz\"]}");
    }

    @Test
    void encodesArrayOfTables() {
        assertThat(eval("return cjson.encode({{['foo'] = 'bar'}})")).isEqualTo("[{\"foo\":\"bar\"}]");
    }

    @Test
    void encodesTableWithNilValueAsEmptyObject() {
        //Lua drops the entry entirely, so the table is empty and encodes as an object.
        assertThat(eval("return cjson.encode({['foo'] = nil})")).isEqualTo("{}");
    }

    @Test
    void encodesEmptyTableAsEmptyObject() {
        assertThat(eval("return cjson.encode({})")).isEqualTo("{}");
    }

    @Test
    void encodesDocsExample() {
        assertThat(eval("return cjson.encode({ ['foo'] = 'bar' })")).isEqualTo("{\"foo\":\"bar\"}");
    }

    @Test
    void encodesFlatArrayOfScalars() {
        assertThat(eval("return cjson.encode({1, 2, 3})")).isEqualTo("[1,2,3]");
        assertThat(eval("return cjson.encode({'a', true, 1.5})")).isEqualTo("[\"a\",true,1.5]");
    }

    @Test
    void encodesNullValueInsideAnArray() {
        //serializeNulls: a nil inside an array reaches the writer as a JSON null.
        assertThat(eval("return cjson.encode({['foo'] = {1, nil}})")).isEqualTo("{\"foo\":[1]}");
    }

    // ---------------------------------------------------------------- encode: nesting

    @Test
    void encodesDeeplyButLegallyNestedInput() {
        //1000 levels is real Redis's cjson encode depth limit, and must keep working.
        String script = "local t = {}\n"
                + "local cur = t\n"
                + "for i = 2, 1000 do\n"
                + "  local child = {}\n"
                + "  cur[1] = child\n"
                + "  cur = child\n"
                + "end\n"
                + "return cjson.encode(t)";
        StringBuilder expected = new StringBuilder();
        for (int i = 0; i < 999; i++) {
            expected.append('[');
        }
        expected.append("{}");
        for (int i = 0; i < 999; i++) {
            expected.append(']');
        }
        assertThat(eval(script)).isEqualTo(expected.toString());
    }

    @Test
    @Timeout(30)
    void rejectsOverlyNestedOrCyclicTablesWithAnErrorReplyRatherThanHanging() {
        //Conversion is recursive, so without a bound this exhausts the stack. A
        //StackOverflowError is an Error: neither the command dispatcher nor pcall catches one,
        //so the client would get no reply at all and the connection would be wedged. It must be
        //a normal error, one level beyond the legal depth.
        String overLimit = "local t = {}\n"
                + "local cur = t\n"
                + "for i = 2, 1001 do\n"
                + "  local child = {}\n"
                + "  cur[1] = child\n"
                + "  cur = child\n"
                + "end\n"
                + "return cjson.encode(t)";
        assertThatThrownBy(() -> eval(overLimit)).isInstanceOf(JedisDataException.class);
        //A table that references itself recurses without ever reaching a base case; it must hit
        //the same bound rather than hang or exhaust the stack.
        String cyclic = "local t = {}\nt.self = t\nreturn cjson.encode(t)";
        assertThatThrownBy(() -> eval(cyclic)).isInstanceOf(JedisDataException.class);
        //The failure is catchable from Lua like any other encode error...
        assertThat(eval("local t = {}\nt.self = t\nlocal ok, _ = pcall(cjson.encode, t)\nreturn ok == false"))
                .isEqualTo(1L);
        //...and the connection is still alive and in sync afterwards.
        assertThat(jedis.ping()).isEqualTo("PONG");
    }

    @Test
    void encodesWideButShallowTableWithoutHittingTheNestingLimit() {
        //Breadth is not depth: the nesting counter must be given back once a container's
        //conversion finishes, or a legal wide table would trip a limit meant for depth alone.
        String script = "local t = {}\nfor i = 1, 2000 do t[i] = {1} end\nreturn cjson.encode(t)";
        String expected = "[" + String.join(",", Collections.nCopies(2000, "[1]")) + "]";
        assertThat(eval(script)).isEqualTo(expected);
    }

    // ---------------------------------------------------------------- encode: map keys

    @Test
    void encodesIntegerKeysAsStrings() {
        //Documented deviation (see CJsonTest#evalCjsonEncodeJustArrayTest, @Disabled):
        //a table with a hole is not an array, so it becomes an object with stringified keys.
        assertThat(eval("return cjson.encode({1, nil, 3})")).isEqualTo("{\"1\":1,\"3\":3}");
    }

    @Test
    void encodesMixedArrayWithHoleAsObject() {
        //Documented deviation (see CJsonTest#evalCjsonEncodeArrayWithMixedTypesTest, @Disabled).
        //Also pins the iteration order of the underlying map for integer keys.
        assertThat(eval("return cjson.encode({1, 'str', true, nil, {1, 2}, {['foo'] = 'bar'}})"))
                .isEqualTo("{\"1\":1,\"2\":\"str\",\"3\":true,\"5\":[1,2],\"6\":{\"foo\":\"bar\"}}");
    }

    @Test
    void encodesFractionalKeyUsingItsDoubleRendering() {
        assertThat(eval("local t = {}\nt[1.5] = 'x'\nreturn cjson.encode(t)"))
                .isEqualTo("{\"1.5\":\"x\"}");
    }

    @Test
    void rejectsBooleanKey() {
        String script = "local invalid_map = {}\n"
                + "invalid_map[false] = 'false'\n"
                + "local ok, _ = pcall(cjson.encode, invalid_map)\n"
                + "return ok == false";
        assertThat(eval(script)).isEqualTo(1L);
    }

    @Test
    void rejectsTableKey() {
        String script = "local invalid_map = {}\n"
                + "invalid_map[{}] = 'table'\n"
                + "local ok, _ = pcall(cjson.encode, invalid_map)\n"
                + "return ok == false";
        assertThat(eval(script)).isEqualTo(1L);
    }

    // ---------------------------------------------------------------- encode: numbers

    @Test
    void encodesIntegralNumbersWithoutADecimalPoint() {
        assertThat(eval("return cjson.encode(0)")).isEqualTo("0");
        assertThat(eval("return cjson.encode(-1)")).isEqualTo("-1");
        assertThat(eval("return cjson.encode(2^53)")).isEqualTo("9007199254740992");
    }

    @Test
    void encodesLargeNonIntegralNumbersInScientificNotation() {
        //Anything Lua does not report as a long is rendered by Double.toString.
        assertThat(eval("return cjson.encode(1e20)")).isEqualTo("1.0E20");
        assertThat(eval("return cjson.encode(3.0e-4)")).isEqualTo("3.0E-4");
    }

    @Test
    void rejectsNonFiniteNumbers() {
        assertThat(eval("local ok, _ = pcall(cjson.encode, 1/0)\nreturn ok == false")).isEqualTo(1L);
        assertThat(eval("local ok, _ = pcall(cjson.encode, 0/0)\nreturn ok == false")).isEqualTo(1L);
    }

    // ---------------------------------------------------------------- encode: string escaping

    @Test
    void escapesQuoteAndBackslash() {
        assertThat(eval("return cjson.encode('a\"b\\\\c')")).isEqualTo("\"a\\\"b\\\\c\"");
    }

    @Test
    void escapesControlCharacters() {
        assertThat(eval("return cjson.encode('a\\nb')")).isEqualTo("\"a\\nb\"");
        assertThat(eval("return cjson.encode('a\\rb')")).isEqualTo("\"a\\rb\"");
        assertThat(eval("return cjson.encode('a\\tb')")).isEqualTo("\"a\\tb\"");
        assertThat(eval("return cjson.encode('a\\8b')")).isEqualTo("\"a\\bb\"");
        assertThat(eval("return cjson.encode('a\\12b')")).isEqualTo("\"a\\fb\"");
        assertThat(eval("return cjson.encode('a\\1b')")).isEqualTo("\"a\\u0001b\"");
        assertThat(eval("return cjson.encode('a\\31b')")).isEqualTo("\"a\\u001fb\"");
    }

    @Test
    void doesNotEscapeForwardSlash() {
        assertThat(eval("return cjson.encode('a/b')")).isEqualTo("\"a/b\"");
    }

    @Test
    void escapesHtmlSignificantCharacters() {
        //Inherited from gson's html-safe default. Real Redis does not do this, but changing it
        //would be a behaviour change, so it is pinned here rather than quietly "fixed".
        assertThat(eval("return cjson.encode([[<>&=']])"))
                .isEqualTo("\"\\u003c\\u003e\\u0026\\u003d\\u0027\"");
    }

    @Test
    void escapesLineAndParagraphSeparators() {
        assertThat(eval("return cjson.encode('\\226\\128\\168')")).isEqualTo("\"\\u2028\"");
        assertThat(eval("return cjson.encode('\\226\\128\\169')")).isEqualTo("\"\\u2029\"");
    }

    @Test
    void leavesOtherNonAsciiCharactersUnescaped() {
        assertThat(eval("return cjson.encode('\\195\\169')")).isEqualTo("\"\u00e9\"");
    }

    // ---------------------------------------------------------------- decode: scalars

    @Test
    void decodesBoolean() {
        assertThat(eval("return cjson.decode('true')")).isEqualTo(1L);
        //Lua false becomes a nil bulk reply.
        assertThat(eval("return cjson.decode('false')")).isNull();
    }

    @Test
    void decodesInteger() {
        assertThat(eval("return cjson.decode('100')")).isEqualTo(100L);
    }

    @Test
    void decodesDouble() {
        assertThat(eval("return tostring(cjson.decode('100.01'))")).isEqualTo("100.01");
    }

    @Test
    void decodesQuotedString() {
        assertThat(eval("return cjson.decode('\"str\"')")).isEqualTo("str");
    }

    @Test
    void decodesNull() {
        assertThat(eval("return cjson.decode('null')")).isNull();
    }

    @Test
    void decodesTopLevelQuotedStringWithoutUnescaping() {
        //Quirk: a bare quoted string takes a fast path that just strips the quotes, so escape
        //sequences survive verbatim instead of being decoded.
        assertThat(eval("return cjson.decode([[\"a\\u0041b\"]])")).isEqualTo("a\\u0041b");
    }

    // ---------------------------------------------------------------- decode: containers

    @Test
    void decodesObjectWithBooleanValue() {
        assertThat(eval("return cjson.decode('{\"foo\":true}')['foo']")).isEqualTo(1L);
    }

    @Test
    void decodesObjectWithNullValue() {
        assertThat(eval("return cjson.decode('{\"foo\":null}')['foo']")).isNull();
    }

    @Test
    void decodesDeeplyNestedArraysAndObjects() {
        assertThat(eval("return cjson.decode('[{\"foo\":[{\"bar\":\"baz\"}]}]')[1]['foo'][1]['bar']"))
                .isEqualTo("baz");
    }

    @Test
    void decodesNestedObject() {
        assertThat(eval("return cjson.decode('{\"foo\":{\"bar\":\"baz\"}}')['foo']['bar']"))
                .isEqualTo("baz");
    }

    @Test
    void decodesArrayOfObjects() {
        assertThat(eval("return cjson.decode('[{\"foo\":\"bar\"}]')[1]['foo']")).isEqualTo("bar");
    }

    @Test
    void decodesDocsExample() {
        assertThat(eval("return cjson.decode('{\"foo\":\"bar\"}')['foo']")).isEqualTo("bar");
    }

    @Test
    void decodesEmptyObject() {
        assertThat(eval("return cjson.decode('{}')")).isEqualTo(Collections.emptyList());
    }

    @Test
    void decodesEmptyArray() {
        assertThat(eval("return cjson.decode('[]')")).isEqualTo(Collections.emptyList());
    }

    @Test
    void decodesEmptyStringValue() {
        assertThat(eval("return cjson.decode('{\"foo\":\"\"}')['foo'] == ''")).isEqualTo(1L);
    }

    @Test
    void decodesNestedStringEscapes() {
        //Unlike the top-level fast path, nested strings really are unescaped.
        assertThat(eval("return cjson.decode([[{\"foo\":\"a\\u0041b\"}]])['foo']")).isEqualTo("aAb");
        assertThat(eval("return cjson.decode([[{\"foo\":\"q\\\"s\\\\t\\/u\"}]])['foo']"))
                .isEqualTo("q\"s\\t/u");
        assertThat(eval("return cjson.decode([[{\"foo\":\"a\\nb\\tc\\rd\\be\\ff\"}]])['foo']"))
                .isEqualTo("a\nb\tc\rd\be\ff");
    }

    @Test
    void decodesLastValueOfADuplicateKey() {
        assertThat(eval("return cjson.decode('{\"foo\":1,\"foo\":2}')['foo']")).isEqualTo(2L);
    }

    @Test
    void ignoresInsignificantWhitespace() {
        assertThat(eval("return cjson.decode(' \\t\\r\\n { \"foo\" : [ 1 , 2 ] } \\n ')['foo'][2]"))
                .isEqualTo(2L);
    }

    // ---------------------------------------------------------------- decode: numbers

    @Test
    void decodesNestedNumbersAsDoubles() {
        //Nested numbers all arrive as doubles; luaj prints integral doubles without a
        //fractional part, and non-integral ones through Float.toString.
        String script = "return table.concat(\n"
                + "cjson.decode(\n"
                + "\"[0.0, -5e3, -1, 1023.2, 0e10]\"), \" \")";
        assertThat(eval(script)).isEqualTo("0 -5000 -1 1023.2 0");
    }

    @Test
    void decodesScientificNotationWithoutConvertingToDecimal() {
        //Documented deviation (see CJsonTest#evalCjsonDecodeScientificNotationNumber, @Disabled):
        //real Redis prints 0.0003.
        assertThat(eval("return table.concat(cjson.decode('[3.0e-4]'), ' ')")).isEqualTo("3.0E-4");
    }

    @Test
    void decodesNumberVariants() {
        assertThat(eval("return table.concat(cjson.decode('[-0.5, 1E+2, 12345678901234, 0]'), ' ')"))
                .isEqualTo("-0.5 100 12345678901234 0");
    }

    @Test
    void treatsNullInAnArrayAsTheEndOfTheArray() {
        //Documented deviation (see CJsonTest#evalCjsonDecodeJsonArrayWithNilTest, @Disabled):
        //real Redis keeps the hole, Lua tables cannot.
        String script = "local decoded = cjson.decode('{\"foo\":[1, null, 3]}')\n"
                + "return decoded['foo']";
        assertThat(eval(script)).isEqualTo(Collections.singletonList(1L));
    }

    // ---------------------------------------------------------------- decode: errors

    @Test
    void rejectsNilArgument() {
        assertThatThrownBy(() -> eval("return cjson.decode(nil)")).isInstanceOf(JedisDataException.class);
    }

    @Test
    void rejectsBareWord() {
        assertThatThrownBy(() -> eval("return cjson.decode('invalid')")).isInstanceOf(JedisDataException.class);
    }

    @Test
    void rejectsTruncatedObject() {
        assertThatThrownBy(() -> eval("return cjson.decode('{\"foo\":\"bar\"')"))
                .isInstanceOf(JedisDataException.class);
    }

    @Test
    void rejectsTruncatedString() {
        assertThatThrownBy(() -> eval("return cjson.decode('{\"foo\":\"bar}')"))
                .isInstanceOf(JedisDataException.class);
    }

    @Test
    void rejectsTrailingGarbage() {
        assertThatThrownBy(() -> eval("return cjson.decode('{\"foo\":1} trailing')"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> eval("return cjson.decode('[1,2] [3]')"))
                .isInstanceOf(JedisDataException.class);
    }

    @Test
    void rejectsUnclosedArray() {
        assertThatThrownBy(() -> eval("return cjson.decode('[1,2')")).isInstanceOf(JedisDataException.class);
    }

    @Test
    void decodesDeeplyButLegallyNestedInput() {
        //255 levels is the deepest gson accepted, and must keep working.
        String script = "local json = string.rep('[', 255) .. string.rep(']', 255)\n"
                + "return type(cjson.decode(json))";
        assertThat(eval(script)).isEqualTo("table");
    }

    @Test
    @Timeout(30)
    void rejectsOverlyNestedInputWithAnErrorReplyRatherThanHanging() {
        //Parsing is recursive, so without a bound this exhausts the stack. A StackOverflowError
        //is an Error: neither the command dispatcher nor pcall catches one, so the client would
        //get no reply at all and the connection would be wedged. It must be a normal error.
        assertThatThrownBy(() -> eval("return cjson.decode(string.rep('[', 256) .. string.rep(']', 256))"))
                .isInstanceOf(JedisDataException.class);
        //Unbalanced input is enough to trigger it: the recursion happens on the opening brackets.
        assertThatThrownBy(() -> eval("return cjson.decode(string.rep('[', 20000))"))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> eval("return cjson.decode(string.rep('{\"a\":', 20000))"))
                .isInstanceOf(JedisDataException.class);
        //The failure is catchable from Lua like any other decode error...
        assertThat(eval("local ok, _ = pcall(cjson.decode, string.rep('[', 20000))\nreturn ok == false"))
                .isEqualTo(1L);
        //...and the connection is still alive and in sync afterwards.
        assertThat(jedis.ping()).isEqualTo("PONG");
    }

    @Test
    void allowsUnlimitedSiblingsAtALegalDepth() {
        //Breadth is not depth: the nesting counter must be given back when a container closes.
        String script = "local parts = {}\n"
                + "for i = 1, 2000 do parts[i] = '[1]' end\n"
                + "return #cjson.decode('[' .. table.concat(parts, ',') .. ']')";
        assertThat(eval(script)).isEqualTo(2000L);
    }

    @Test
    void decodeFailureIsCatchableFromLua() {
        assertThat(eval("local ok, _ = pcall(cjson.decode, 'invalid')\nreturn ok == false"))
                .isEqualTo(1L);
    }

    // ---------------------------------------------------------------- round trip / library

    @Test
    void roundTripsASmokeMap() {
        String script = "local some_map = {\n"
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
        assertThat(eval(script)).isEqualTo(1L);
    }

    @Test
    void roundTripsValuesThroughEncodeAndDecode() {
        String script = "local original = {['s'] = 'a\"b/c', ['n'] = 100.01, ['b'] = true,\n"
                + "                  ['a'] = {1, 2, 3}, ['o'] = {['k'] = 'v'}}\n"
                + "local d = cjson.decode(cjson.encode(original))\n"
                + "return {d['s'], tostring(d['n']), tostring(d['b']), d['a'][3], d['o']['k']}";
        assertThat(eval(script))
                .isEqualTo(Arrays.asList("a\"b/c", "100.01", "true", 3L, "v"));
    }

    @Test
    void cjsonTableIsReadOnly() {
        assertThatThrownBy(() -> eval("cjson.encode = function() return 1 end"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageContaining("Attempt to modify a readonly table");
    }
}
