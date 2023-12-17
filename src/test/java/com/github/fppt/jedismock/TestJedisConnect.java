package com.github.fppt.jedismock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;

import java.io.IOException;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

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
    public void testPipeline() {
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
            Pipeline pl = jedis.pipelined();
            pl.set("a", "abc");
            pl.get("a");
            List<Object> resp = pl.syncAndReturnAll();
            assertThat(resp).contains("OK", "abc");
            jedis.disconnect();
        }
    }

    @Test
    public void testMultipleClient() {
        try (Jedis jedis1 = new Jedis(server.getHost(), server.getBindPort());
             Jedis jedis2 = new Jedis(server.getHost(), server.getBindPort())) {
            assertThat(jedis1.set("a", "b")).isEqualTo("OK");
            assertThat(jedis2.get("a")).isEqualTo("b");
            jedis1.disconnect();
            jedis2.disconnect();
        }
    }

    @Test
    public void testLpush() {
        try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort(), 10000000)) {
            assertThat(jedis.lpush("list", "world")).isEqualTo(1);
            assertThat(jedis.lpush("list", "hello")).isEqualTo(2);
            assertThat(jedis.rpush("list", "!")).isEqualTo(3);
            assertThat(jedis.lrange("list", 0, -1)).containsExactly("hello", "world", "!");
            jedis.disconnect();
        }
    }
}
