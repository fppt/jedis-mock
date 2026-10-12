package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandRegistries;
import com.github.fppt.jedismock.operations.RedisCommand;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.CommandInfo;

import java.util.Map;
import java.util.TreeMap;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every {@link RedisCommand} annotation declares about its command must
 * match what the real server reports for it through {@code COMMAND}: the
 * arity, in Redis' own terms, and whether the command writes (Redis' {@code
 * write} flag, the one read-only scripts check).
 */
@Testcontainers
public class CommandParityTest {
    @Container
    private final GenericContainer<?> redis = new RedisContainer().withExposedPorts(6379);

    private static String describe(long arity, boolean readOnly) {
        return "arity = " + arity + ", readOnly = " + readOnly;
    }

    @Test
    public void everyCommandDeclaresWhatRealRedisReports() {
        Map<String, CommandInfo> real;
        try (Jedis jedis = new Jedis(redis.getHost(), redis.getFirstMappedPort())) {
            real = jedis.command();
        }

        Map<String, String> declared = new TreeMap<>();
        Map<String, String> expected = new TreeMap<>();
        CommandRegistries.commands.forEach((name, command) -> {
            CommandInfo info = real.get(name);
            assertThat(info).as("%s is unknown to real Redis", name).isNotNull();
            declared.put(name, describe(command.arity(), command.readOnly()));
            expected.put(name, describe(info.getArity(), !info.getFlags().contains("write")));
        });
        assertThat(declared).containsExactlyEntriesOf(expected);
    }
}
