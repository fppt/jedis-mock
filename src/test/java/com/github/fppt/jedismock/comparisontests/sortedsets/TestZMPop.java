package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.args.SortedSetOption;
import redis.clients.jedis.resps.Tuple;
import redis.clients.jedis.util.KeyValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestZMPop {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushDB();

    }

    @TestTemplate
    public void testZMPopKeyNotExist(Jedis jedis) {
        assertNull(jedis.zmpop(SortedSetOption.MIN, "aaa"));
    }

    @TestTemplate
    public void testZMPopFromOneKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY, Collections.singletonList(new Tuple("one", 1.0)));
        assertEquals(expected, jedis.zmpop(SortedSetOption.MIN, ZSET_KEY));

        List<Tuple> tupleList = Arrays.asList(new Tuple("two", 2.), new Tuple("three", 3.));
        assertEquals(tupleList, jedis.zrangeWithScores(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testZMPopCount(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY,
                Arrays.asList(new Tuple("three", 3.0),
                              new Tuple("two", 2.0)));
        assertEquals(expected, jedis.zmpop(SortedSetOption.MAX, 10, ZSET_KEY));
    }

    @TestTemplate
    public void testZMPopKeyNegativeCount(Jedis jedis) {
        assertThrows(RuntimeException.class, () ->
                jedis.zmpop(SortedSetOption.MIN, -1, "aaa")
        );
    }

}
