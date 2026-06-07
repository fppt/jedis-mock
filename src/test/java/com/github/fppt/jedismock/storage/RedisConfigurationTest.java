package com.github.fppt.jedismock.storage;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class RedisConfigurationTest {

    @Test
    void unsetParameterReadsBackAsEmpty() {
        RedisConfiguration config = new RedisConfiguration();
        assertThat(config.get("requirepass")).isEmpty();
    }

    @Test
    void setValueRoundTripsCaseInsensitively() {
        RedisConfiguration config = new RedisConfiguration();
        config.set("Maxmemory-Policy", "allkeys-lru");
        assertThat(config.get("maxmemory-policy")).isEqualTo("allkeys-lru");
    }

    @Test
    void protoMaxBulkLenDefaultsTo512Mb() {
        assertThat(new RedisConfiguration().getProtoMaxBulkLen()).isEqualTo(512L * 1024 * 1024);
    }

    @Test
    void protoMaxBulkLenIsCappedAtIntegerMaxValue() {
        // The mock stores strings as byte[], so it cannot honour a multi-GB
        // limit; capping it is also what makes real Redis's large-memory tests
        // (set proto-max-bulk-len to 10gb, then check it round-tripped) self-skip.
        RedisConfiguration config = new RedisConfiguration();
        config.setProtoMaxBulkLen(10_000_000_000L);
        assertThat(config.getProtoMaxBulkLen()).isEqualTo(Integer.MAX_VALUE);
    }
}
