package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TestZRemRangeByScore {

    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void clearKey(Jedis jedis) {
        jedis.del(ZSET_KEY);
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItReturnsZeroForNonDefinedKey(Jedis jedis) {
        assertEquals(0, jedis.zremrangeByScore(ZSET_KEY, "-inf", "+inf"));
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItClearsEverythingWithPlusMinusInfinity(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 1, "two");
        jedis.zadd(ZSET_KEY, 1, "three");

        assertEquals(3, jedis.zremrangeByScore(ZSET_KEY, "-inf", "+inf"));
        assertEquals(0, jedis.zrange(ZSET_KEY, 0, -1).size());
    }


    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItReturnsSetSizeWhenLowestAndHighestScoresSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, "-inf", "+inf");

        // then
        assertEquals(3, zremrangeByScoreResult);
        assertEquals(0, jedis.zrange(ZSET_KEY, 0, -1).size());
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItRemovesValueWhenIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, "-inf", "2");

        // then
        assertEquals(2, zremrangeByScoreResult);
        final List<String> zrangeResult = jedis.zrange(ZSET_KEY, 0, -1);
        assertEquals(1, zrangeResult.size());
        assertArrayEquals(zrangeResult.toArray(), new String[]{"three"});
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItDoesNotRemoveValueWhenExclusiveIntervalSpecified(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        assertEquals(3, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, "-inf", "(2");

        // then
        assertEquals(1, zremrangeByScoreResult);
        final List<String> zrangeResult = jedis.zrange(ZSET_KEY, 0, -1);
        assertEquals(2, zrangeResult.size());
        assertEquals(Arrays.asList("two", "three"), zrangeResult);
    }

    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItRemovesValuesAccordingToSpecifiedInterval(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        jedis.zadd(ZSET_KEY, 4, "four");
        jedis.zadd(ZSET_KEY, 5, "five");
        jedis.zadd(ZSET_KEY, 6, "six");
        jedis.zadd(ZSET_KEY, 7, "seven");
        jedis.zadd(ZSET_KEY, 8, "eight");
        jedis.zadd(ZSET_KEY, 9, "nine");
        jedis.zadd(ZSET_KEY, 10, "ten");
        assertEquals(10, jedis.zrange(ZSET_KEY, 0, -1).size());

        // when
        final Long zremrangeByScoreResult = jedis.zremrangeByScore(ZSET_KEY, 5, 8);

        // then
        assertEquals(4, zremrangeByScoreResult);
        final List<String> zrangeResult = jedis.zrange(ZSET_KEY, 0, -1);
        assertEquals(6, zrangeResult.size());
        assertEquals(Arrays.asList("one", "two", "three", "four", "nine", "ten"), zrangeResult);
    }


    @TestTemplate
    public void whenUsingZremrangeByScore_EnsureItThrowsExceptionsWhenStartAndEndHaveWrongFormat(Jedis jedis) {
        // given
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");

        // then
        assertThrows(JedisDataException.class,
                () -> jedis.zremrangeByScore(ZSET_KEY, "(dd", "(sd"));
        assertThrows(JedisDataException.class,
                () -> jedis.zremrangeByScore(ZSET_KEY, "1.e", "2.d"));
        assertThrows(RuntimeException.class,
                () -> jedis.zremrangeByScore(ZSET_KEY, "FOO", "BAR"));
    }
}
