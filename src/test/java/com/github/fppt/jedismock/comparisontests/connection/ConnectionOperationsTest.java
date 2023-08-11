package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ComparisonBase.class)
public class ConnectionOperationsTest {

    @TestTemplate
    public void whenUsingQuit_EnsureTheResultIsOK(Jedis jedis, HostAndPort hostAndPort) {
        //Create a new connection
        try (Jedis newJedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort())) {
            newJedis.set("A happy lucky key", "A sad value");
            assertEquals("OK", newJedis.quit());
            assertEquals("A sad value", jedis.get("A happy lucky key"));
        }
    }

    @TestTemplate
    public void whenPinging_Pong(Jedis jedis) {
        assertEquals("PONG", jedis.ping());
        assertEquals("foo", jedis.ping("foo"));
    }

    @TestTemplate
    public void echo(Jedis jedis) {
        assertEquals("foobar", jedis.echo("foobar"));
    }

    @TestTemplate
    public void whenSettingClientName_EnsureOkResponseIsReturned(Jedis jedis) {
        assertNull(jedis.clientGetname());
        assertEquals("OK", jedis.clientSetname("P.Myo"));
        assertEquals("P.Myo", jedis.clientGetname());
    }
}
