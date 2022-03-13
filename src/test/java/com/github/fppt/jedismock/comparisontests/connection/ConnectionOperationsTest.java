package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class ConnectionOperationsTest {

    @TestTemplate
    public void whenUsingQuit_EnsureTheResultIsOK(Jedis jedis, HostAndPort hostAndPort) {
        //Create a new connection
        Jedis newJedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        newJedis.set("A happy lucky key", "A sad value");

        assertEquals("OK", newJedis.quit());
        assertEquals("A sad value", jedis.get("A happy lucky key"));
    }

    @TestTemplate
    public void whenPinging_Pong(Jedis jedis) {
        assertEquals("PONG", jedis.ping());
    }

    @TestTemplate
    public void whenSettingClientName_EnsureOkResponseIsReturned(Jedis jedis) {
        assertEquals("OK", jedis.clientSetname("P.Myo"));
    }
}
