package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.resps.Tuple;

import java.util.Map;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRandMember {

    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void zrandMemberReturnsSingleElement(Jedis jedis) {
        jedis.zadd("myzset", Map.of("a", 1.0, "b", 2.0, "c", 3.0));
        String member = jedis.zrandmember("myzset");
        assertThat(Set.of("a", "b", "c")).contains(member);
    }

    @TestTemplate
    void zrandMemberReturnsNullIfZSetEmpty(Jedis jedis) {
        assertThat(jedis.zrandmember("nonexistent")).isNull();
    }

    @TestTemplate
    void zrandMemberWithCountReturnsEmptyListForNonexistentKey(Jedis jedis) {
        assertThat(jedis.zrandmember("myzset", 2)).isEmpty();
    }

    @TestTemplate
    void zrandMemberWithCountReturnsElements(Jedis jedis) {
        jedis.zadd("myzset", Map.of("a", 1.0, "b", 2.0, "c", 3.0));
        List<String> members = jedis.zrandmember("myzset", 2);
        assertThat(members.size()).isLessThanOrEqualTo(2);
        assertThat(Set.of("a", "b", "c")).containsAll(members);
    }

    @TestTemplate
    void zrandMemberWithNegativeCountReturnsRepeatedElements(Jedis jedis) {
        jedis.zadd("myzset", Map.of("x", 1.0, "y", 2.0, "z", 3.0));
        List<String> members = jedis.zrandmember("myzset", -10);
        assertThat(members).hasSize(10);
        assertThat(Set.of("x", "y", "z")).containsAll(members);
    }

    @TestTemplate
    void zrandMemberWithCountAndWithScores(Jedis jedis) {
        jedis.zadd("myzset", Map.of("apple", 5.0, "banana", 7.0, "carrot", 9.0));
        List<Tuple> result = jedis.zrandmemberWithScores("myzset", 2);
        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getScore()).isIn(5.0, 7.0, 9.0);
        assertThat(result.get(1).getScore()).isIn(5.0, 7.0, 9.0);

        assertThat(result.get(0).getElement()).isIn("apple", "banana", "carrot");
        assertThat(result.get(1).getElement()).isIn("apple", "banana", "carrot");
    }

    @TestTemplate
    void zrandMemberReturnsAllWhenCountExceedsSet(Jedis jedis) {
        jedis.zadd("myzset", Map.of("1", 1.0, "2", 2.0, "3", 3.0));
        List<String> members = jedis.zrandmember("myzset", 10);
        assertThat(members).containsExactlyInAnyOrder("1", "2", "3");
    }

    @TestTemplate
    void zrandMemberZeroCountReturnsEmptyList(Jedis jedis) {
        jedis.zadd("myzset", Map.of("1", 1.0, "2", 2.0));
        List<String> result = jedis.zrandmember("myzset", 0);
        assertThat(result).isEmpty();
    }

    @TestTemplate
    void zrandMemberWithInvalidWithScoresThrowsError(Jedis jedis) {
        jedis.zadd("myzset", Map.of("x", 1.0));
        try {
            jedis.sendCommand(Protocol.Command.ZRANDMEMBER, "myzset", "1", "WRONGARG");
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("ERR syntax error");
        }
    }
}
