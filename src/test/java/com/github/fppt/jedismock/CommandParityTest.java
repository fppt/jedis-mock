package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandDescriptor;
import com.github.fppt.jedismock.operations.CommandRegistries;
import com.github.fppt.jedismock.operations.RedisCommand;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.CommandInfo;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * What every {@link RedisCommand} annotation declares about its command must
 * match what the real server reports for it through {@code COMMAND}: the
 * arity, in Redis' own terms, and whether the command writes (Redis' {@code
 * write} flag, the one read-only scripts check).
 *
 * <p>One invocation per registered command, so a mismatch names the command
 * in the test report rather than burying it in a map diff.
 */
@Testcontainers
public class CommandParityTest {

    private static Map<String, CommandInfo> real;

    @BeforeAll
    static void collectWhatRealRedisReports() {
        try (GenericContainer<?> realRedis = new RedisContainer().withExposedPorts(6379)) {
            realRedis.start();
            try (Jedis jedis = new Jedis(realRedis.getHost(), realRedis.getFirstMappedPort())) {
                real = jedis.command();
            }
        }
    }

    static Stream<Arguments> commands() {
        return CommandRegistries.commands
                .values()
                .stream()
                .sorted(Comparator.comparing(CommandDescriptor::name))
                .map(x -> Arguments.arguments(x.name().toUpperCase(), x));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("commands")
    void declaresWhatRealRedisReports(String name, CommandDescriptor command) {
        CommandInfo info = real.get(command.name());
        assertThat(info).as("%s is unknown to real Redis", command.name()).isNotNull();
        assertThat((long) command.arity()).as("arity of %s", command.name()).isEqualTo(info.getArity());
        assertThat(command.readOnly())
                .as("readOnly of %s (Redis reports flags %s)", command.name(), info.getFlags())
                .isEqualTo(!info.getFlags().contains("write"));
    }
}
