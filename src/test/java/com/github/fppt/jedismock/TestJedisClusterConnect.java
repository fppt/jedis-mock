package com.github.fppt.jedismock;

import com.github.fppt.jedismock.server.ServiceOptions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestJedisClusterConnect {

    RedisServer server;

    @BeforeEach
    void setup() throws IOException {
        server = RedisServer
                .newRedisServer()
                .setOptions(ServiceOptions.defaultOptions().withClusterModeEnabled())
                .start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }

    @Test
    void jedisClusterClientCanConnectAndWork() {
        Set<HostAndPort> jedisClusterNodes = new HashSet<>();
        jedisClusterNodes.add(
                new HostAndPort(server.getHost(), server.getBindPort()));

        String[] planets = new String[]{"Mars", "Jupyter", "Venus", "Earth", "Mercury", "Saturn"};
        try (JedisCluster jedis = new JedisCluster(jedisClusterNodes)) {
            jedis.sadd("planets", planets);
            assertThat(jedis.smembers("planets")).containsExactlyInAnyOrder(planets);
            assertThat(jedis.getClusterNodes()).hasSize(1);
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    void helloReportsClusterMode() {
        try (Jedis jedis = new Jedis(new HostAndPort(server.getHost(), server.getBindPort()))) {
            //The reply is a flat array of alternating field names and values.
            List<Object> hello = (List<Object>) jedis.sendCommand(Protocol.Command.HELLO, "2");
            String mode = null;
            for (int i = 0; i + 1 < hello.size(); i += 2) {
                if ("mode".equals(SafeEncoder.encode((byte[]) hello.get(i)))) {
                    mode = SafeEncoder.encode((byte[]) hello.get(i + 1));
                }
            }
            assertThat(mode).isEqualTo("cluster");
        }
    }

    @Test
    void selectOperationDoesNotWorkInClusterMode() {
        try (Jedis jedis = new Jedis(new HostAndPort(server.getHost(), server.getBindPort()))) {
            assertThatThrownBy(() -> jedis.select(1))
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage("ERR SELECT is not allowed in cluster mode");
        }
    }
}
