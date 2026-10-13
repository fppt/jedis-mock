package com.github.fppt.jedismock.comparisontests.functions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisBusyException;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static com.github.fppt.jedismock.comparisontests.functions.FunctionUtils.getNoWritesFunctionLibraryCode;
import static com.github.fppt.jedismock.comparisontests.functions.FunctionUtils.getSingleFunctionLibraryCode;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class FCallRoTest {

    private static final String READ_ONLY_ERROR = "ERR Write commands are not allowed from read-only scripts.";
    private static final String WRITE_FLAG_ERROR = "ERR Can not execute a script with write flag using *_ro command.";

    @BeforeEach
    void flush(Jedis jedis) {
        jedis.flushAll();
        jedis.functionFlush();
    }

    @TestTemplate
    void rejectsFunctionWithoutNoWritesFlag(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return 'hello'"));
        assertThatThrownBy(() -> jedis.fcallReadonly("test", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(WRITE_FLAG_ERROR);
        //Plain FCALL is unaffected.
        assertThat(jedis.fcall("test", List.of(), List.of())).isEqualTo("hello");
    }

    @TestTemplate
    void noWritesFunctionCanRead(Jedis jedis) {
        jedis.functionLoad(getNoWritesFunctionLibraryCode("LUA", "test", "test", "return redis.call('get', KEYS[1])"));
        jedis.set("x", "1");
        assertThat(jedis.fcallReadonly("test", List.of("x"), List.of())).isEqualTo("1");
        assertThat(jedis.fcall("test", List.of("x"), List.of())).isEqualTo("1");
    }

    @TestTemplate
    void noWritesFunctionCannotWrite(Jedis jedis) {
        jedis.functionLoad(getNoWritesFunctionLibraryCode("LUA", "test", "test", "return redis.call('set', 'x', '1')"));
        assertThatThrownBy(() -> jedis.fcallReadonly("test", List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(READ_ONLY_ERROR + " script: test, on @user_function:3.");
        //The no-writes flag is enforced by plain FCALL as well.
        assertThatThrownBy(() -> jedis.fcall("test", List.of("x"), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(READ_ONLY_ERROR + " script: test, on @user_function:3.");
        assertThat(jedis.get("x")).isNull();
    }

    @TestTemplate
    void pcallOfWriteCommandReturnsErrorTable(Jedis jedis) {
        jedis.functionLoad(getNoWritesFunctionLibraryCode("LUA", "test", "test",
                "local t = redis.pcall('set', 'x', '1') return t['err']"));
        assertThat(jedis.fcallReadonly("test", List.of("x"), List.of())).isEqualTo(READ_ONLY_ERROR);
        assertThat(jedis.get("x")).isNull();
    }

    @TestTemplate
    void selectIsAllowed(Jedis jedis) {
        jedis.functionLoad(getNoWritesFunctionLibraryCode("LUA", "test", "test",
                "redis.call('select', 1) return redis.call('get', 'x')"));
        jedis.set("x", "1");
        assertThat(jedis.fcallReadonly("test", List.of(), List.of())).isNull();
        assertThat(jedis.get("x")).isEqualTo("1");
    }

    @TestTemplate
    void rejectsCallToNonExistentFunction(Jedis jedis) {
        assertThatThrownBy(() -> jedis.fcallReadonly("does_not_exist", List.of(), List.of()))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }

    @TestTemplate
    void functionLookupPrecedesNumKeysValidation(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL_RO, "does_not_exist", "-1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL, "does_not_exist", "-1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Function not found");
    }

    @TestTemplate
    void numKeysValidationPrecedesWriteFlagCheck(Jedis jedis) {
        jedis.functionLoad(getSingleFunctionLibraryCode("LUA", "test", "test", "return redis.call('set', 'x', '1')"));
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL_RO, "test", "-1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of keys can't be negative");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL_RO, "test", "2", "key"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Number of keys can't be greater than number of args");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL_RO, "test", "abc"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Bad number of keys provided");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL_RO, "test", "0"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage(WRITE_FLAG_ERROR);
    }

    @TestTemplate
    void arityIsValidated(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.FCALL_RO, "test"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR wrong number of arguments for 'fcall_ro' command");
    }

    @TestTemplate
    void isNotAllowedFromScripts(Jedis jedis) {
        jedis.functionLoad(getNoWritesFunctionLibraryCode("LUA", "test", "test", "return 1"));
        assertThatThrownBy(() -> jedis.eval("return redis.call('fcall_ro', 'test', 0)", 0))
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR This (Redis )?command is not allowed from script script: .*");
    }

    @TestTemplate
    void functionStatsReportsInvokingCommand(Jedis jedis, HostAndPort hostAndPort) {
        jedis.configSet("busy-reply-threshold", "1");
        jedis.functionLoad(getNoWritesFunctionLibraryCode("LUA", "test", "test", "local a = 1 while true do a = a + 1 end"));

        Jedis busyClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService pool = Executors.newSingleThreadExecutor();
        try {
            pool.submit(() -> busyClient.fcallReadonly("test", List.of("my-key"), List.of("my-value")));

            Awaitility.await().atMost(Duration.ofSeconds(5)).untilAsserted(() ->
                    assertThatThrownBy(jedis::ping).isInstanceOf(JedisBusyException.class));

            assertThat(jedis.functionStats().getRunningScript())
                    .containsEntry("name", "test")
                    .containsEntry("command", List.of("FCALL_RO", "test", "1", "my-key", "my-value"));
        } finally {
            try {
                jedis.functionKill();
                jedis.ping();
            } finally {
                pool.shutdownNow();
                busyClient.close();
            }
        }
    }
}
