package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.resps.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class SortedSetOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void zcardEmptyKey(Jedis jedis) {
        String key = "mykey";

        long result = jedis.zcard(key);

        assertThat(result).isEqualTo(0L);
    }

    @TestTemplate
    public void zcardReturnsCount(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        jedis.zadd(key, members);

        long result = jedis.zcard(key);

        assertThat(result).isEqualTo(2L);
    }

    @TestTemplate
    public void zrangeKeysCorrectOrder(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue2", 10d);
        members.put("myvalue4", 20d);
        members.put("myvalue3", 15d);
        members.put("myvalue1", 9d);

        long result = jedis.zadd(key, members);

        assertThat(result).isEqualTo(4L);

        List<String> results = jedis.zrange(key, 0, -1);
        assertThat(results).containsExactly("myvalue1", "myvalue2", "myvalue3", "myvalue4");
    }

    @TestTemplate
    public void zrangeIndexOutOfRange(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue2", 10d);
        members.put("myvalue4", 20d);
        members.put("myvalue3", 15d);
        members.put("myvalue1", 9d);

        long result = jedis.zadd(key, members);

        assertThat(result).isEqualTo(4L);

        List<String> results = jedis.zrange(key, 0, -6);

        assertThat(results).isEmpty();
    }

    @TestTemplate
    public void zrangeWithScores(Jedis jedis) {
        String key = "mykey";
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue2", 10d);
        members.put("myvalue4", 20d);
        members.put("myvalue3", 15d);
        members.put("myvalue1", 9d);

        long result = jedis.zadd(key, members);

        assertThat(result).isEqualTo(4L);

        List<Tuple> results = jedis.zrangeWithScores(key, 0, -1);

        assertThat(results).containsExactly(new Tuple("myvalue1", 9d),
                new Tuple("myvalue2", 10d),
                new Tuple("myvalue3", 15d),
                new Tuple("myvalue4", 20d));
    }

    @TestTemplate
    public void zscore(Jedis jedis) {
        String key = "a_key";
        Map<String, Double> members = new HashMap<>();
        members.put("aaa", 0d);
        members.put("bbb", 1d);
        members.put("ddd", 1d);

        long result = jedis.zadd(key, members);
        assertThat(result).isEqualTo(3L);

        assertThat(jedis.zscore(key, "aaa")).isEqualTo(0d);
        assertThat(jedis.zscore(key, "bbb")).isEqualTo(1d);
        assertThat(jedis.zscore(key, "ddd")).isEqualTo(1d);
        assertThat(jedis.zscore(key, "ccc")).isNull();
    }

    @TestTemplate
    public void testGetOperation(Jedis jedis) {
        String key = "a_key";
        Map<String, Double> members = new HashMap<>();
        members.put("aaa", 0d);
        members.put("bbb", 1d);
        members.put("ddd", 1d);
        jedis.zadd(key, members);
        assertThatThrownBy(() -> jedis.get("a_key"))
                .isInstanceOf(JedisDataException.class);
    }
}
