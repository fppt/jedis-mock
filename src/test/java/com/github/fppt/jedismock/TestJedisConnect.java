package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by Xiaolu on 2015/4/21.
 */
public class TestJedisConnect {
    RedisServer server;

    @BeforeEach
    void setup() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }

    @Test
    public void testPipeline() throws IOException {
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            Pipeline pl = jedis.pipelined();
            pl.set("a", "abc");
            pl.get("a");
            List<Object> resp = pl.syncAndReturnAll();
            assertEquals(resp.get(0), "OK");
            assertEquals(resp.get(1), "abc");
            jedis.disconnect();
        }
    }

    @Test
    public void testMultipleClient() throws IOException {
        try (Jedis jedis1 = new Jedis(server.getHost(), server.getBindPort());
             Jedis jedis2 = new Jedis(server.getHost(), server.getBindPort());) {
            assertEquals(jedis1.set("a", "b"), "OK");
            assertEquals(jedis2.get("a"), "b");
            jedis1.disconnect();
            jedis2.disconnect();
        }
    }

    @Test
    public void testLpush() throws IOException {
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort(), 10000000)) {
            assertEquals(1, jedis.lpush("list", "world"));
            assertEquals(2, jedis.lpush("list", "hello"));
            assertEquals(3, jedis.rpush("list", "!"));
            assertArrayEquals(new String[]{"hello", "world", "!"}, jedis.lrange("list", 0, -1).toArray());
            jedis.disconnect();
        }
    }
}
