package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class TestInterceptor {
    @Test
    public void testMockedOperations() throws IOException {
        RedisServer server = RedisServer
                .newRedisServer()
                .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
                    //You can write any verification code here
                    assertEquals("set", roName.toLowerCase());
                    //You can can imitate any reply from Redis here
                    return Response.bulkString(Slice.create("MOCK"));
                }))
                .start();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            String result = jedis.set("a", "b");
            assertEquals("MOCK", result);
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
            assertEquals(jedis.set("ab", "cd"), "OK");
            assertEquals(jedis.set("ab", "cd"), "OK");
            assertEquals(jedis.set("ab", "cd"), "OK");
            assertThrows(JedisConnectionException.class, () -> jedis.set("ab", "cd"));
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
                        assertEquals("hello", params.get(0).toString());
                        //And imitate connection breaking
                        return MockExecutor.breakConnection(state);
                    } else {
                        //Delegate execution to JedisMock which will mock the real Redis behaviour (when it can)
                        return MockExecutor.proceed(state, roName, params);
                    }
                }))
                .start();
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            assertEquals("MOCK_VALUE", jedis.get("foo"));
            assertEquals("OK", jedis.set("bar", "baz"));
            assertThrows(JedisConnectionException.class, () -> jedis.echo("hello"));
        }
        server.stop();
    }
}
