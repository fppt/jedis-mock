package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import static redis.clients.jedis.params.LPosParams.lPosParams;

@ExtendWith(ComparisonBase.class)
public class LPosTest {

    private static final String key = "lpos_key";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();

        jedis.rpush(key, "a", "b", "c", "1", "2", "3", "c", "c");
    }

    @TestTemplate
    @DisplayName("Check for basic usage")
    public void whenUsingLPos_EnsureReturnsLeftmostEntry(Jedis jedis) {
        assertThat(jedis.lpos(key, "c")).isEqualTo(2);
        assertThat(jedis.lpos(key, "3")).isEqualTo(5);
        assertThat(jedis.lpos(key, "a")).isEqualTo(0);
        assertThat(jedis.lpos(key, "d")).isNull();
    }

    @TestTemplate
    @DisplayName("Check for rank param")
    public void whenUsingLPos_EnsureRankWorksCorrectly(Jedis jedis) {
        assertThat(jedis.lpos(key, "c", lPosParams().rank(1))).isEqualTo(2);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(2))).isEqualTo(6);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-1))).isEqualTo(7);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-2))).isEqualTo(6);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(4))).isNull();
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-4))).isNull();

        assertThatThrownBy(() -> jedis.lpos(key, "c", lPosParams().rank(0)))
                .isInstanceOf(JedisDataException.class);
    }

    @TestTemplate
    @DisplayName("Check for count param")
    public void whenUsingLPos_EnsureCountWorksCorrectly(Jedis jedis) {
        assertThat(jedis.lpos(key, "c", lPosParams(), 1)).containsExactly(2L);
        assertThat(jedis.lpos(key, "c", lPosParams(), 2)).containsExactly(2L, 6L);
        assertThat(jedis.lpos(key, "c", lPosParams(), 3)).containsExactly(2L, 6L, 7L);
        assertThat(jedis.lpos(key, "c", lPosParams(), 0)).containsExactly(2L, 6L, 7L);
    }

    @TestTemplate
    @DisplayName("Check for maxlen param")
    public void whenUsingLPos_EnsureMaxlenWorksCorrectly(Jedis jedis) {
        assertThat(jedis.lpos(key, "1", lPosParams().maxlen(3))).isNull();
        assertThat(jedis.lpos(key, "1", lPosParams().maxlen(4))).isEqualTo(3L);
        assertThat(jedis.lpos(key, "1", lPosParams().maxlen(0))).isEqualTo(3L);
    }

    @TestTemplate
    @DisplayName("Check for count and rank params combined")
    public void whenUsingLPos_EnsureCountAndRankWorkCorrectly(Jedis jedis) {
        assertThat(jedis.lpos(key, "c", lPosParams().rank(2), 2)).containsExactly(6L, 7L);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-1), 2)).containsExactly(7L, 6L);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-1), 0)).containsExactly(7L, 6L, 2L);
    }

    @TestTemplate
    @DisplayName("Check for rank and maxlen params cobined")
    public void whenUsingLPos_EnsureRankAndMaxlenWorkCorrectly(Jedis jedis) {
        assertThat(jedis.lpos(key, "c", lPosParams().rank(1).maxlen(3))).isEqualTo(2L);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(1).maxlen(2))).isNull();
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-1).maxlen(1))).isEqualTo(7L);
        assertThat(jedis.lpos(key, "3", lPosParams().rank(-1).maxlen(1))).isNull();
        assertThat(jedis.lpos(key, "3", lPosParams().rank(-1).maxlen(3))).isEqualTo(5L);
    }

    @TestTemplate
    @DisplayName("Check for all params combined")
    public void whenUsingLPos_EnsureAllParamsWorkCorrectly(Jedis jedis) {
        assertThat(jedis.lpos(key, "c", lPosParams().rank(1).maxlen(5), 2)).containsExactly(2L);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(1).maxlen(7), 2)).containsExactly(2L, 6L);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-1).maxlen(1), 2)).containsExactly(7L);
        assertThat(jedis.lpos(key, "c", lPosParams().rank(-1).maxlen(2), 2)).containsExactly(7L, 6L);
    }
}
