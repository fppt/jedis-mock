package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.ZAddParams;
import redis.clients.jedis.resps.Tuple;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestZadd {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void zaddAddsKey(Jedis jedis) {
        double score = 10;
        String value = "myvalue";

        long result = jedis.zadd(ZSET_KEY, score, value);

        assertThat(result).isEqualTo(1L);

        List<String> results = jedis.zrange(ZSET_KEY, 0, -1);

        assertThat(results).containsExactly(value);
    }

    @TestTemplate
    public void zaddAddsKeys(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("myvalue1", 10d);
        members.put("myvalue2", 20d);

        long result = jedis.zadd(ZSET_KEY, members);

        assertThat(result).isEqualTo(2L);

        List<String> results = jedis.zrange(ZSET_KEY, 0, -1);

        assertThat(results).containsExactly("myvalue1", "myvalue2");
    }

    @TestTemplate
    public void testZaddNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.zadd("foo".getBytes(), 42, msg);
        byte[] newMsg = jedis.zrange("foo".getBytes(), 0, 0).get(0);
        assertThat(newMsg).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testBasicZAddAndScoreUpdate(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10d, "x");
        jedis.zadd(ZSET_KEY, 20d, "y");
        jedis.zadd(ZSET_KEY, 30d, "z");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).containsExactly("x", "y", "z");

        jedis.zadd(ZSET_KEY, 1d, "y");
        assertThat(jedis.zrange(ZSET_KEY, 0, -1)).containsExactly("y", "x", "z");
    }

    @TestTemplate
    public void testZAddKeys(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("a", 10d);
        members.put("b", 20d);
        members.put("c", 30d);

        jedis.zadd(ZSET_KEY, members);
        List<Tuple> results = jedis.zrangeWithScores(ZSET_KEY, 0, -1);
        assertThat(results).containsExactly(new Tuple("a", 10.0),
                new Tuple("b", 20.0),
                new Tuple("c", 30.0));
    }

    @TestTemplate
    public void testZAddXXWithoutKey(Jedis jedis) {
        long result = jedis.zadd(ZSET_KEY, 10, "x", new ZAddParams().xx());

        assertThat(result).isEqualTo(0);
        assertThat(jedis.type(ZSET_KEY)).isEqualTo("none");
    }

    @TestTemplate
    public void testZAddXXToExistKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10, "x");
        long result = jedis.zadd(ZSET_KEY, 20, "y", new ZAddParams().xx());

        assertThat(result).isEqualTo(0);
        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(1);
    }

    @TestTemplate
    public void testZAddGetNumberAddedElements(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 10, "x");
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);
        long result = jedis.zadd(ZSET_KEY, members);

        assertThat(result).isEqualTo(2);
    }

    @TestTemplate
    public void testZAddXXUpdateExistingScores(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("foo", 5d);
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("zap", 40d);
        jedis.zadd(ZSET_KEY, members2, new ZAddParams().xx());

        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(3);
        assertThat(jedis.zscore(ZSET_KEY, "x")).isEqualTo(11);
        assertThat(jedis.zscore(ZSET_KEY, "y")).isEqualTo(21);
    }

    @TestTemplate
    public void testZAddXXandNX(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zadd(ZSET_KEY, 10, "x", new ZAddParams().xx().nx()))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void testZAddLTandGT(Jedis jedis) {
        assertThatThrownBy(() -> jedis.zadd(ZSET_KEY, 10, "x", new ZAddParams().lt().gt()))
                .isInstanceOf(RuntimeException.class);
    }

    @TestTemplate
    public void testZAddNXWithNonExistingKey(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);

        jedis.zadd(ZSET_KEY, members, new ZAddParams().nx());

        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(3);
    }

    @TestTemplate
    public void testZAddNXOnlyAddNewElements(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);
        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("a", 100d);
        members2.put("b", 200d);

        long result = jedis.zadd(ZSET_KEY, members2, new ZAddParams().nx());

        assertThat(result).isEqualTo(2);
        assertThat(jedis.zscore(ZSET_KEY, "x")).isEqualTo(10);
        assertThat(jedis.zscore(ZSET_KEY, "y")).isEqualTo(20);
        assertThat(jedis.zscore(ZSET_KEY, "a")).isEqualTo(100);
        assertThat(jedis.zscore(ZSET_KEY, "b")).isEqualTo(200);
    }

    @TestTemplate
    public void testZAddIncrLikeIncrBy(Jedis jedis) {
        Map<String, Double> members = new HashMap<>();
        members.put("x", 10d);
        members.put("y", 20d);
        members.put("z", 30d);
        jedis.zadd(ZSET_KEY, members);
        jedis.zaddIncr(ZSET_KEY, 15, "x", new ZAddParams());

        assertThat(jedis.zscore(ZSET_KEY, "x")).isEqualTo(25);
    }

    @TestTemplate
    public void testZAddIncrToNotExistKey(Jedis jedis) {
        assertThat(jedis.zaddIncr(ZSET_KEY, 15, "x", new ZAddParams())).isEqualTo(15);
    }

    @TestTemplate
    public void testZAddCHGetNumberChangedElements(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("z", 30d);
        assertThat(jedis.zadd(ZSET_KEY, members2)).isEqualTo(0);

        Map<String, Double> members3 = new HashMap<>();
        members3.put("x", 12d);
        members3.put("y", 22d);
        members3.put("z", 30d);
        assertThat(jedis.zadd(ZSET_KEY, members3, new ZAddParams().ch())).isEqualTo(2);
    }

    @TestTemplate
    public void testZAddGTXXCH(Jedis jedis) {
        Map<String, Double> members1 = new HashMap<>();
        members1.put("x", 10d);
        members1.put("y", 20d);
        members1.put("z", 30d);
        jedis.zadd(ZSET_KEY, members1);

        Map<String, Double> members2 = new HashMap<>();
        members2.put("foo", 5d);
        members2.put("x", 11d);
        members2.put("y", 21d);
        members2.put("z", 29d);
        assertThat(jedis.zadd(ZSET_KEY, members2, new ZAddParams().gt().xx().ch())).isEqualTo(2);

        assertThat(jedis.zcard(ZSET_KEY)).isEqualTo(3);
        assertThat(jedis.zscore(ZSET_KEY, "x")).isEqualTo(11);
        assertThat(jedis.zscore(ZSET_KEY, "y")).isEqualTo(21);
        assertThat(jedis.zscore(ZSET_KEY, "z")).isEqualTo(30);
    }

    @TestTemplate
    public void testZAddIncrLTIfScoreNotUpdate(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 28, "x");

        assertThat(jedis.zaddIncr(ZSET_KEY, 1, "x", new ZAddParams().lt())).isNull();
        assertThat(jedis.zscore(ZSET_KEY, "x")).isEqualTo(28);
    }

    @TestTemplate
    public void testZAddIncrGTIfScoreNotUpdate(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 28, "x");

        assertThat(jedis.zaddIncr(ZSET_KEY, -1, "x", new ZAddParams().gt())).isNull();
        assertThat(jedis.zscore(ZSET_KEY, "x")).isEqualTo(28);
    }

}
