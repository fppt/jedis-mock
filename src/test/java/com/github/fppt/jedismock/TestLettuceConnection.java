package com.github.fppt.jedismock;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class TestLettuceConnection {

    @Test
    void lettuceClientCanConnectAndWork() throws Exception {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        try {
            RedisClient redisClient = RedisClient
                    .create(String.format("redis://%s:%s",
                            server.getHost(), server.getBindPort()));
            StatefulRedisConnection<String, String> connection = redisClient.connect();
            RedisCommands<String, String> syncCommands = connection.sync();
            syncCommands.set("key", "Hello, Redis!");
            String val = syncCommands.get("key");
            connection.close();
            redisClient.shutdown();

            assertEquals("Hello, Redis!", val);
        } finally {
            server.stop();
        }
    }
}
