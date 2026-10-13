package com.github.fppt.jedismock.comparisontests.functions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class FunctionFlagsTest {

    @BeforeEach
    void flush(Jedis jedis) {
        jedis.flushAll();
        jedis.functionFlush();
    }

    private static String libraryWithFlags(String flags) {
        return "#!LUA name=test\n"
                + "redis.register_function{function_name='f1', callback=function(KEYS, ARGV) "
                + "return redis.call('set', 'x', '1') end, flags=" + flags + "}";
    }

    @SuppressWarnings("unchecked")
    private static List<Object> listedFlags(Jedis jedis) {
        assertThat(jedis.functionList()).hasSize(1);
        Map<String, Object> function = (Map<String, Object>) jedis.functionList().get(0).getFunctions().get(0);
        return (List<Object>) function.get("flags");
    }

    @TestTemplate
    void flagsAreListedInDefinitionOrderCaseInsensitively(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{'allow-cross-slot-keys', 'NO-WRITES', 'allow-oom'}"));
        assertThat(listedFlags(jedis)).containsExactly("no-writes", "allow-oom", "allow-cross-slot-keys");
    }

    @TestTemplate
    void allKnownFlagsAreAccepted(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{'no-writes', 'allow-oom', 'allow-stale', 'no-cluster', 'allow-cross-slot-keys'}"));
        assertThat(listedFlags(jedis))
                .containsExactly("no-writes", "allow-oom", "allow-stale", "no-cluster", "allow-cross-slot-keys");
    }

    @TestTemplate
    void duplicateFlagIsListedOnce(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{'no-writes', 'no-writes'}"));
        assertThat(listedFlags(jedis)).containsExactly("no-writes");
    }

    @TestTemplate
    void emptyFlagsTableIsAccepted(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{}"));
        assertThat(listedFlags(jedis)).isEmpty();
        assertThat(jedis.fcall("f1", List.of(), List.of())).isEqualTo("OK");
    }

    @TestTemplate
    void onlyTheArrayPartOfTheFlagsTableIsRead(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{a='no-writes'}"));
        assertThat(listedFlags(jedis)).isEmpty();
        assertThat(jedis.fcall("f1", List.of(), List.of())).isEqualTo("OK");
    }

    @TestTemplate
    void unknownFlagsAreRejected(Jedis jedis) {
        for (String flags : List.of("{'bogus'}", "{1}", "{true}", "{{}}", "{'no-writes', 5}")) {
            assertThatThrownBy(() -> jedis.functionLoad(libraryWithFlags(flags)))
                    .as(flags)
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage("ERR Error registering functions: ERR unknown flag given");
        }
    }

    @TestTemplate
    void flagsMustBeATable(Jedis jedis) {
        assertThatThrownBy(() -> jedis.functionLoad(libraryWithFlags("'no-writes'")))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Error registering functions: ERR flags argument to redis.register_function "
                        + "must be a table representing function flags");
    }

    @TestTemplate
    void noWritesFlagMakesFunctionReadOnly(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{'no-writes'}"));
        assertThatThrownBy(() -> jedis.fcall("f1", List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Write commands are not allowed from read-only scripts. script: f1, on @user_function:2.");
        assertThat(jedis.get("x")).isNull();
    }

    @TestTemplate
    void otherFlagsDoNotRestrictWrites(Jedis jedis) {
        jedis.functionLoad(libraryWithFlags("{'allow-oom', 'allow-stale', 'no-cluster', 'allow-cross-slot-keys'}"));
        assertThat(jedis.fcall("f1", List.of("x"), List.of())).isEqualTo("OK");
        assertThat(jedis.get("x")).isEqualTo("1");
    }
}
