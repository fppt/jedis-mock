package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Thin {@code CONFIG} support (issue #40): clients such as Lettuce and Redisson
 * issue {@code CONFIG} under the hood and must not fail with "unsupported
 * operation". The mock stores parameters in a separate namespace and round-trips
 * them, without otherwise changing behaviour.
 */
@ExtendWith(ComparisonBase.class)
public class ConfigComparisonTest {

    @TestTemplate
    public void getOfUnsetParameterReturnsEmptyValue(Jedis jedis) {
        Map<String, String> result = jedis.configGet("requirepass");
        assertThat(result).containsEntry("requirepass", "");
    }

    @TestTemplate
    public void setThenGetRoundTripsTheValue(Jedis jedis) {
        assertThat(jedis.configSet("maxmemory-policy", "allkeys-lru")).isEqualTo("OK");
        assertThat(jedis.configGet("maxmemory-policy"))
                .containsEntry("maxmemory-policy", "allkeys-lru");
        // Restore the default so the shared real-Redis container isn't left altered.
        jedis.configSet("maxmemory-policy", "noeviction");
    }
}
