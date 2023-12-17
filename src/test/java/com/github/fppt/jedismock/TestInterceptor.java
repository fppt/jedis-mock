package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class TestInterceptor {
    @Test
    public void testMockedOperations() throws IOException {
        RedisServer server = RedisServer
                .newRedisServer()
                .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
                    //You can write any verification code here
                    assertThat(roName.toLowerCase()).isEqualTo("set");
                    //You can imitate any reply from Redis here
                    return Response.bulkString(Slice.create("MOCK"));
                }))
                .start();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            String result = jedis.set("a", "b");
            assertThat(result).isEqualTo("MOCK");
        }
        server.stop();
    }

    @Test
    public void testCloseSocket() throws IOException {
        RedisServer server = RedisServer
                .newRedisServer()
                .setOptions(ServiceOptions.executeOnly(3))
                .start();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            assertThat(jedis.set("ab", "cd")).isEqualTo("OK");
            assertThat(jedis.set("ab", "cd")).isEqualTo("OK");
            assertThat(jedis.set("ab", "cd")).isEqualTo("OK");
            assertThatThrownBy(() -> jedis.set("ab", "cd"))
                    .isInstanceOf(JedisConnectionException.class);
        }
        server.stop();
    }

    @Test
    public void moreComplexExample() throws IOException {
        RedisServer server = RedisServer
                .newRedisServer()
                .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
                    if ("get".equalsIgnoreCase(roName)) {
                        //You can can imitate any reply from Redis
                        return Response.bulkString(Slice.create("MOCK_VALUE"));
                    } else if ("echo".equalsIgnoreCase(roName)) {
                        //You can write any verification code
                        assertThat(params.get(0).toString()).isEqualTo("hello");
                        //And imitate connection breaking
                        return MockExecutor.breakConnection(state);
                    } else {
                        //Delegate execution to JedisMock which will mock the real Redis behaviour (when it can)
                        return MockExecutor.proceed(state, roName, params);
                    }
                }))
                .start();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            assertThat(jedis.get("foo")).isEqualTo("MOCK_VALUE");
            assertThat(jedis.set("bar", "baz")).isEqualTo("OK");
            assertThatThrownBy(() -> jedis.echo("hello"))
                    .isInstanceOf(JedisConnectionException.class);
        }
        server.stop();
    }
}
