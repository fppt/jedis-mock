package com.github.fppt.jedismock;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.net.BindException;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class TestRedisServer {

    @Test
    public void testBindPort() throws IOException {
        RedisServer server = RedisServer.newRedisServer(8080);
        server.start();
        assertEquals(server.getBindPort(), 8080);
        server.stop();
    }

    @Test
    public void testBindRandomPort() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        server.stop();
    }

    @Test
    public void testBindErrorPort() throws IOException {
        RedisServer server = RedisServer.newRedisServer(100000);
        try {
            server.start();
            fail();
        } catch (IllegalArgumentException e) {
            // OK
        }
    }

    @Test
    public void testBindUsedPort() throws IOException {
        RedisServer server1 = RedisServer.newRedisServer();
        server1.start();
        RedisServer server2 = RedisServer.newRedisServer(server1.getBindPort());
        try {
            server2.start();
            fail();
        } catch (BindException e) {
            // OK
        }
    }

    @Test
    public void whenRepeatedlyStoppingAndStartingServer_EnsureItResponds() throws IOException {
        for (int i = 0; i < 20; i ++){
            RedisServer server = RedisServer.newRedisServer();
            server.start();

            Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
            assertEquals("PONG", jedis.ping());

            server.stop();
            assertThrows(JedisConnectionException.class, () -> {
                jedis.ping();
            });
        }

        RedisServer server = RedisServer.newRedisServer();
        for (int i = 0; i < 20; i ++){
            server.start();

            Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
            assertEquals("PONG", jedis.ping());

            server.stop();
            assertThrows(JedisConnectionException.class, () -> {
                jedis.ping();
            });
        }
    }
}
