package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;


@ExtendWith(ComparisonBase.class)
public class AdvanceConnectionOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenChangingBetweenRedisDBS_EnsureChangesAreMutuallyExclusive(Jedis jedis) {
        String key1 = "k1";
        String key2 = "k2";

        String val1 = "v1";
        String val2 = "v2";
        String val3 = "v3";

        //Mess With Default Cluster
        jedis.set(key1, val1);
        jedis.set(key2, val2);
        assertThat(jedis.get(key1)).isEqualTo(val1);
        assertThat(jedis.get(key2)).isEqualTo(val2);

        //Change to new DB
        jedis.select(2);
        assertThat(jedis.get(key1)).isNull();
        assertThat(jedis.get(key2)).isNull();

        jedis.set(key1, val3);
        jedis.set(key2, val3);
        assertThat(jedis.get(key1)).isEqualTo(val3);
        assertThat(jedis.get(key2)).isEqualTo(val3);

        //Change back and make sure original is unchanged
        jedis.select(0);
        assertThat(jedis.get(key1)).isEqualTo(val1);
        assertThat(jedis.get(key2)).isEqualTo(val2);
    }
}
