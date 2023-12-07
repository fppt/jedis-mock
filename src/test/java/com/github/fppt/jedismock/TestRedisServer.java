package com.github.fppt.jedismock;

import org.junit.jupiter.api.Test;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.IOException;
import java.net.BindException;

import static com.github.fppt.jedismock.RedisServer.newRedisServer;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class TestRedisServer {

    @Test
    public void testBindPort() throws IOException {
        RedisServer server = RedisServer.newRedisServer(8080);
        server.start();
        assertThat(server.getBindPort()).isEqualTo(8080);
        server.stop();
    }

    @Test
    public void testBindRandomPort() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        server.stop();
    }

    @Test
    public void testBindErrorPort() {
        RedisServer server = RedisServer.newRedisServer(100000);
        assertThatThrownBy(server::start)
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    public void testBindUsedPort() throws IOException {
        RedisServer server1 = newRedisServer();
        server1.start();
        RedisServer server2 = newRedisServer(server1.getBindPort());
        assertThatThrownBy(server2::start)
                .isInstanceOf(BindException.class);
    }

    @Test
    public void whenRepeatedlyStoppingAndCreatingServer_EnsureItResponds() throws IOException {
        for (int i = 0; i < 20; i++) {
            RedisServer server = RedisServer.newRedisServer();
            server.start();
            try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
                assertThat(jedis.ping()).isEqualTo("PONG");
                server.stop();
                assertThatThrownBy(jedis::ping)
                        .isInstanceOf(JedisConnectionException.class);
            }
        }
    }

    @Test
    public void whenPartOfTheClientsQuitAndServerStops_AllTheConnectionsAreClosed() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        server.start();
        Jedis[] jedis = new Jedis[5];
        for (int i = 0; i < jedis.length; i++) {
            jedis[i] = new Jedis(server.getHost(), server.getBindPort());
            assertThat(jedis[i].ping()).isEqualTo("PONG");
            if (i % 2 == 1) {
                //Part of the clients quit
                jedis[i].quit();
            }
        }
        server.stop();
        for (Jedis j : jedis) {
            assertThatThrownBy(j::ping)
                    .isInstanceOf(JedisConnectionException.class);
            j.close();
        }
    }

    @Test
    public void whenRepeatedlyStoppingAndStartingServer_EnsureItResponds() throws IOException {
        RedisServer server = RedisServer.newRedisServer();
        for (int i = 0; i < 20; i++) {
            server.start();
            try (Jedis jedis = new Jedis(server.getHost(), server.getBindPort())) {
                assertThat(jedis.ping()).isEqualTo("PONG");
                server.stop();
                assertThatThrownBy(jedis::ping)
                        .isInstanceOf(JedisConnectionException.class);
            }
        }
    }
}
