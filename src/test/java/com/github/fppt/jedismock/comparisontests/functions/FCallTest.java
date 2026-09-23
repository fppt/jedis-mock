package com.github.fppt.jedismock.comparisontests.functions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static com.github.fppt.jedismock.comparisontests.functions.FunctionUtils.getSingleFunctionLibraryCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class FCallTest {

    @BeforeEach
    void flush(Jedis jedis) {
        jedis.functionFlush();
    }

    @TestTemplate
    void basicUsage(Jedis jedis) {
        assertThat(jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello'"))).isEqualTo("test");
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello");
    }

    @TestTemplate
    void functionNamesAreCaseInsensitive(Jedis jedis) {
        assertThat(List.of(new String[]{"test", "TEST"}, new String[]{"TEST", "test"})).allSatisfy(names -> {
            jedis.functionFlush();
            assertThat(jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", names[0], "return 'hello'"))).isEqualTo("test");
            assertThat(jedis.fcall(names[1], List.of(), List.of())).isEqualTo("hello");
        });
    }

    @TestTemplate
    void rejectsCallToNonExistentFunction(Jedis jedis) {
        assertThatThrownBy(() ->
                jedis.fcall("does_not_exist", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }

    @TestTemplate
    void rejectsCallWithInvalidNumKeys(Jedis jedis) {
        assertThat(jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello'"))).isEqualTo("test");

        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.FCALL, "test"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'fcall' command");
        assertThat(List.of(
                new String[]{"test", ""},
                new String[]{"test", "bad_arg"}
        )).allSatisfy(args ->
                assertThatThrownBy(() ->
                        jedis.sendCommand(Protocol.Command.FCALL, args))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Bad number of keys provided")
        );
        assertThat(List.of(
                new String[]{"test", "1"},
                new String[]{"test", "2", "key"}
        )).allSatisfy(args ->
                assertThatThrownBy(() ->
                        jedis.sendCommand(Protocol.Command.FCALL, args))
                        .isInstanceOf(JedisDataException.class)
                        .hasMessage("ERR Number of keys can't be greater than number of args")
        );
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.FCALL, "test", "-1", "key"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of keys can't be negative");
    }

    @TestTemplate
    void keysAndArgsAreProvidedCorrectly(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return redis.call('set', KEYS[1], ARGV[1])"));
        assertThat(jedis.fcall("test", List.of("x"), List.of("foo"))).isEqualTo("OK");
        assertThat(jedis.get("x")).isEqualTo("foo");
    }

    @TestTemplate
    void keysAndArgsAreBinarySafe(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return {KEYS[1], ARGV[1]}"));
        byte[] key = {'a', 0, 'b', (byte) 0xff};
        byte[] arg = {(byte) 0xfe, 1, 'c'};

        Object result = jedis.fcall("test".getBytes(), List.of(key), List.of(arg));

        assertThat(result).isInstanceOf(List.class);
        List<?> values = (List<?>) result;
        assertThat(values.get(0)).isEqualTo(key);
        assertThat(values.get(1)).isEqualTo(arg);
    }

    @TestTemplate
    void statusReplyIsAvailable(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test",
                "return redis.status_reply('Everything is fine')"));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("Everything is fine");
    }

    @TestTemplate
    void errorReplyIsAvailable(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test",
                "return redis.error_reply('Something bad happened')"));
        assertThatThrownBy(() -> jedis.fcall("test", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("Something bad happened");
    }

    @TestTemplate
    void logLevelConstantsAreAvailable(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test",
                "return {redis.LOG_DEBUG, redis.LOG_VERBOSE, redis.LOG_NOTICE, redis.LOG_WARNING}"));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo(List.of(0L, 1L, 2L, 3L));
    }

    @TestTemplate
    void logIsAvailable(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test",
                "redis.log(redis.LOG_WARNING, 'Something is wrong')\nreturn 'ok'"));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("ok");
    }

    @TestTemplate
    void pcallIsAvailable(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test",
                """
                        local reply = redis.pcall('RENAME', 'doesnotexist', 'B')
                        if reply['err'] ~= nil then
                          return 'Handled error from pcall'
                        end
                        return reply
                        """
        ));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("Handled error from pcall");
    }

    @TestTemplate
    void sha1hexIsAvailable(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test",
                "return redis.sha1hex('Pizza & Mandolino')"));
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("74822d82031af7493c20eefa13bd07ec4fada82f");
    }

    @TestTemplate
    void redisCallWithNoArgsIsRejected(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return redis.call()"));
        assertThatThrownBy(() -> jedis.fcall("test", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("ERR Please specify at least one argument for this redis lib call script: test");
    }

    @TestTemplate
    void fcallIsDisallowedWithinFunction(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib1", "hi1", "return 'hello'"));
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib2", "hi2", "return redis.call('FCALL', 'hi1', 0)"));
        assertThatThrownBy(() -> jedis.fcall("hi2", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: hi2, on .*");
    }

    @TestTemplate
    void evalIsDisallowedWithinFunction(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib", "hi", "return redis.call('EVAL', 'return \\'hello\\'', 0)"));
        assertThatThrownBy(() -> jedis.fcall("hi", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: hi, on .*");
    }

    @TestTemplate
    void functionIsDisallowedWithinFunction(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "lib", "hi", "return redis.call('FUNCTION', 'LIST')"));
        assertThatThrownBy(() -> jedis.fcall("hi", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: hi, on .*");
    }
}
