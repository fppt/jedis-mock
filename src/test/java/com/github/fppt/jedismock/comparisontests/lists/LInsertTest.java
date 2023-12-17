package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;
import static redis.clients.jedis.args.ListPosition.AFTER;
import static redis.clients.jedis.args.ListPosition.BEFORE;

@ExtendWith(ComparisonBase.class)
public class LInsertTest {

    private final static String key = "linsert_key";
    private final static String nonExistingkey = "linsert_key2";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.rpush(key, "1", "2", "3", "3", "4");
    }

    @TestTemplate
    @DisplayName("Check basic linsert case")
    public void whenUsingLInsert_EnsureCorrectlyInserted(Jedis jedis) {
        assertThat(jedis.linsert(key, AFTER, "1", "10")).isEqualTo(6);
        assertThat(jedis.lrange(key, 0, -1)).containsExactly("1", "10", "2", "3", "3", "4");
    }

    @TestTemplate
    @DisplayName("Check insert before first")
    public void whenUsingLInsert_EnsureCorrectlyInsertedBeforeFirst(Jedis jedis) {
        assertThat(jedis.linsert(key, BEFORE, "1", "10")).isEqualTo(6);
        assertThat(jedis.lrange(key, 0, -1)).containsExactly("10", "1", "2", "3", "3", "4");
    }

    @TestTemplate
    @DisplayName("Check insert after last")
    public void whenUsingLInsert_EnsureCorrectlyInsertedAfterLast(Jedis jedis) {
        assertThat(jedis.linsert(key, AFTER, "4", "10")).isEqualTo(6);
        assertThat(jedis.lrange(key, 0, -1)).containsExactly("1", "2", "3", "3", "4", "10");
    }

    @TestTemplate
    @DisplayName("Check choosing leftmost pivot")
    public void whenUsingLInsert_EnsureCorrectlyChosenLeftmostPivot(Jedis jedis) {
        assertThat(jedis.linsert(key, AFTER, "3", "10")).isEqualTo(6);
        assertThat(jedis.lrange(key, 0, -1)).containsExactly("1", "2", "3", "10", "3", "4");
    }

    @TestTemplate
    @DisplayName("Check non existing key")
    public void whenUsingLInsert_EnsureReturnsZeroOnNonExistingKey(Jedis jedis) {
        assertThat(jedis.linsert(nonExistingkey, AFTER, "1", "1")).isEqualTo(0);
    }

    @TestTemplate
    @DisplayName("Check for no pivot")
    public void whenUsingLInsert_EnsureReturnsNegOneOnPivotNotFound(Jedis jedis) {
        assertThat(jedis.linsert(key, AFTER, "5", "10")).isEqualTo(-1);
    }
}
