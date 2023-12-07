package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class HKeysOperation {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void hkeysUnknownKey(Jedis jedis) {
        Set<String> res = jedis.hkeys("foo");
        assertThat(res).isEmpty();
    }

    @TestTemplate
    void hvalsUnknownKey(Jedis jedis) {
        List<String> res = jedis.hvals("foo");
        assertThat(res).isEmpty();
    }

    @TestTemplate
    void hlenUnknownKey(Jedis jedis) {
        long hlen = jedis.hlen("foo");
        assertThat(hlen).isEqualTo(0);
    }

    @TestTemplate
    void hGetAllUnknownKey(Jedis jedis) {
        Map<String, String> result = jedis.hgetAll("foo");
        assertThat(result).isEmpty();
    }
}
