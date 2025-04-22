package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.util.SafeEncoder;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(ComparisonBase.class)
public class ConnectionOperationsTest {

    @TestTemplate
    public void whenUsingQuit_EnsureTheResultIsOK(Jedis jedis, HostAndPort hostAndPort) {
        //Create a new connection
        try (Jedis newJedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort())) {
            newJedis.set("A happy lucky key", "A sad value");
            assertThat(newJedis.sendCommand(() -> SafeEncoder.encode("QUIT"))).isEqualTo("OK".getBytes());
            assertThat(jedis.get("A happy lucky key")).isEqualTo("A sad value");
        }
    }

    @TestTemplate
    public void whenPinging_Pong(Jedis jedis) {
        assertThat(jedis.ping()).isEqualTo("PONG");
        assertThat(jedis.ping("foo")).isEqualTo("foo");
    }

    @TestTemplate
    public void echo(Jedis jedis) {
        assertThat(jedis.echo("foobar")).isEqualTo("foobar");
    }

    @TestTemplate
    public void whenSettingClientName_EnsureOkResponseIsReturned(Jedis jedis) {
        assertThat(jedis.clientGetname()).isNull();
        assertThat(jedis.clientSetname("P.Myo")).isEqualTo("OK");
        assertThat(jedis.clientGetname()).isEqualTo("P.Myo");
    }
}
