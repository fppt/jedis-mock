package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.ZRangeParams;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static redis.clients.jedis.params.ZRangeParams.zrangeByLexParams;

@ExtendWith(ComparisonBase.class)
public class TestZRangeWithByLexArg {

    private final String key = "mykey";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();
        Map<String, Double> members = new HashMap<>();
        members.put("bbb", 0d);
        members.put("ddd", 0d);
        members.put("ccc", 0d);
        members.put("aaa", 0d);
        long result = jedis.zadd(key, members);
        assertThat(result).isEqualTo(4L);
    }

    @TestTemplate
    public void zrangebylexKeysCorrectOrderUnbounded(Jedis jedis) {
        List<String> results = jedis.zrange(key, ZRangeParams.zrangeByLexParams("-", "+"));
        assertThat(results).containsExactly("aaa", "bbb", "ccc", "ddd");
    }

    @TestTemplate
    void zrangebylexKeysCorrectOrderBounded(Jedis jedis) {
        List<String> results = jedis.zrange(key, ZRangeParams.zrangeByLexParams("[bbb", "(ddd"));
        assertThat(results).containsExactly("bbb", "ccc");
    }

    @TestTemplate
    public void zrevrangebylexKeysCorrectOrderUnbounded(Jedis jedis) {
        List<String> results = jedis.zrange(key, ZRangeParams.zrangeByLexParams("+", "-").rev());
        assertThat(results).containsExactly("ddd", "ccc", "bbb", "aaa");
    }

    @TestTemplate
    void zrevrangebylexKeysCorrectOrderBounded(Jedis jedis) {
        List<String> results = jedis.zrange(key, ZRangeParams.zrangeByLexParams("[ddd", "(bbb").rev());
        assertThat(results).containsExactly("ddd", "ccc");
    }

    @TestTemplate
    public void zrangebylexKeysThrowsOnIncorrectParameters(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zrange(key, zrangeByLexParams("b", "[d")))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zrange(key, zrangeByLexParams("b", "[d").rev()))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zrange(key, zrangeByLexParams("[b", "d")))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.zrange(key, zrangeByLexParams("[b", "d").rev()))
                .isInstanceOf(JedisDataException.class);
    }
}
