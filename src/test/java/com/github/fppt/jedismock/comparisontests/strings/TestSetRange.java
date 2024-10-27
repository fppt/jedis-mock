package com.github.fppt.jedismock.comparisontests.strings;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestSetRange {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void setRange(Jedis jedis) {
        jedis.set("key1", "Hello World");
        final long l = jedis.setrange("key1", 6, "Redis");
        assertThat(l).isEqualTo(11);
        assertThat(jedis.get("key1")).isEqualTo("Hello Redis");
    }

    @TestTemplate
    public void setRangeAppend(Jedis jedis) {
        jedis.set("key1", "Hello World");
        final long l = jedis.setrange("key1", 6, "Redis Redis");
        assertThat(l).isEqualTo(17);
        assertThat(jedis.get("key1")).isEqualTo("Hello Redis Redis");
    }

    @TestTemplate
    public void setRangeInTheMiddle(Jedis jedis) {
        jedis.set("key1", "Hello World");
        final long l = jedis.setrange("key1", 2, "FOO");
        assertThat(l).isEqualTo(11);
        assertThat(jedis.get("key1")).isEqualTo("HeFOO World");
    }


    @TestTemplate
    public void setRangeZeroPadding(Jedis jedis) {
        final long l = jedis.setrange("key2", 6, "Redis");
        assertThat(l).isEqualTo(11);
        assertThat(jedis.get("key2")).isEqualTo(new String(new byte[6]) + "Redis");
    }

    @TestTemplate
    public void setRangeAgainstNonExistent(Jedis jedis) {
        assertThat(jedis.setrange("mykey", 0, "foo"))
                .isEqualTo(3);
        assertThat(jedis.get("mykey")).isEqualTo("foo");
    }

    @TestTemplate
    public void setRangeAgainstNonExistentNoOp(Jedis jedis) {
        assertThat(jedis.setrange("mykey", 0, "")).isEqualTo(0);
        assertThat(jedis.exists("mykey")).isFalse();
    }

    @TestTemplate
    public void setRangeAgainstNonExistentPadding(Jedis jedis) {
        assertThat(jedis.setrange("mykey", 1, "foo")).isEqualTo(4);
        assertThat(jedis.get("mykey")).isEqualTo("\000foo");
    }

    @TestTemplate
    public void setRangeWithOutOfRange(Jedis jedis) {
        assertThatThrownBy(() -> jedis.setrange("mykey", 512 * 1024 * 1024 - 4, "world"))
                .hasMessageStartingWith("ERR string exceeds maximum allowed size (proto-max-bulk-len)");
    }

    @TestTemplate
    public void setRangeWithNegativeOffset(Jedis jedis) {
        assertThatThrownBy(() -> jedis.setrange("mykey", -1, "world"))
                .hasMessageStartingWith("ERR offset is out of range");
    }

}
