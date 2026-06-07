package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Thin {@code CONFIG} support (issue #40): code that issues {@code CONFIG} must
 * not fail with "unsupported operation" — notably Spring Data Redis / Spring
 * Session, which send {@code CONFIG SET notify-keyspace-events} automatically
 * when a key-expiration listener is registered. The mock stores parameters in a
 * separate namespace and round-trips them, without otherwise changing behaviour.
 */
@ExtendWith(ComparisonBase.class)
public class ConfigComparisonTest {

    /**
     * Restore the parameters these tests change to their defaults. In an
     * {@code @AfterEach} (not at the end of each test) so it runs even when a
     * test fails — otherwise a changed value would leak into the shared
     * (per-class) mock and real-Redis servers.
     */
    @AfterEach
    public void restoreDefaults(Jedis jedis) {
        jedis.configSet("proto-max-bulk-len", "536870912");
        jedis.configSet("maxmemory-policy", "noeviction");
    }

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
    }

    /**
     * {@code proto-max-bulk-len} is a behavioural parameter: it caps the size of
     * a string a command may build. 1 MiB is real Redis's minimum, so it round-
     * trips on both, and SETRANGE checks the bound without allocating.
     */
    @TestTemplate
    public void protoMaxBulkLenRoundTripsAndBoundsSetrange(Jedis jedis) {
        assertThat(jedis.configSet("proto-max-bulk-len", "1048576")).isEqualTo("OK");
        assertThat(jedis.configGet("proto-max-bulk-len"))
                .containsEntry("proto-max-bulk-len", "1048576");

        jedis.del("bulk");
        // A result within the limit is fine...
        assertThat(jedis.setrange("bulk", 100, "world")).isEqualTo(105L);
        // ...one that would exceed it is rejected (no large allocation needed).
        assertThatThrownBy(() -> jedis.setrange("bulk", 1048573, "world"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageContaining("maximum allowed size");
    }

    @TestTemplate
    public void appendBeyondProtoMaxBulkLenIsRejected(Jedis jedis) {
        jedis.configSet("proto-max-bulk-len", "1048576");
        jedis.del("big");
        jedis.set("big", "a".repeat(1048576)); // exactly at the limit
        assertThatThrownBy(() -> jedis.append("big", "x"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageContaining("maximum allowed size");
    }
}
