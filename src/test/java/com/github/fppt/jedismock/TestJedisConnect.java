package com.github.fppt.jedismock;

import com.github.fppt.jedismock.server.ServiceOptions;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Created by Xiaolu on 2015/4/21.
 */
public class TestJedisConnect {

    @Test
    public void testPipeline() throws IOException, InterruptedException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        Jedis jedis = new Jedis(server.getHost(), server.getBindPort());
        Pipeline pl = jedis.pipelined();
        pl.set("a", "abc");
        pl.get("a");
        List<Object> resp = pl.syncAndReturnAll();
        assertEquals(resp.get(0), "OK");
        assertEquals(resp.get(1), "abc");
        jedis.disconnect();
        server.stop();
    }

    @Test
    public void testMultipleClient() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        Jedis jedis1 = new Jedis(server.getHost(), server.getBindPort());
        Jedis jedis2 = new Jedis(server.getHost(), server.getBindPort());
        assertEquals(jedis1.set("a", "b"), "OK");
        assertEquals(jedis2.get("a"), "b");
        jedis1.disconnect();
        jedis2.disconnect();
        server.stop();
    }

    @Test
    public void testMockedOperations() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        ServiceOptions options = ServiceOptions.create(3);
        server.setOptions(options);
        server.setMockedCommands((roName, params) -> { return null; } /*Does nothing for unknown commands.*/);
        server.start();
        Jedis jedis = new Jedis(server.getHost(), server.getBindPort());

        //assertEquals(jedis.);

        server.stop();
    }

    @Test
    public void testLpush() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        Jedis jedis = new Jedis(server.getHost(), server.getBindPort(), 10000000);
        assertEquals(1, (long) jedis.lpush("list", "world"));
        assertEquals(2, (long) jedis.lpush("list", "hello"));
        assertEquals(3, (long) jedis.rpush("list", "!"));
        assertArrayEquals(new String[]{"hello", "world", "!"}, jedis.lrange("list", 0, -1).toArray());
        jedis.disconnect();
        server.stop();
    }
}
