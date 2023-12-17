package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class WatchTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenUsingPop_ensureAffectsWatchedKey(Jedis jedis, HostAndPort hostAndPort) {
        String key = "watch_key";
        String setKey = "set_key";
        String initialValue = "initial_value";

        Jedis secondClient = new Jedis(hostAndPort);

        jedis.set(setKey, initialValue);
        jedis.lpush(key, "1", "2", "3");
        jedis.watch(key);

        Transaction t = jedis.multi();
        t.set(setKey, "some_value");
        secondClient.rpop(key);
        t.exec();

        assertThat(jedis.get(setKey)).isEqualTo(initialValue);
    }
}
