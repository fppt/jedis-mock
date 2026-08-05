package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.resps.Tuple;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZRandMember {

    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void zrandMemberReturnsSingleElement(Jedis jedis) {
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("a", 1.0);
            put("b", 2.0);
            put("c", 3.0);
        }});
        String member = jedis.zrandmember("myzset");
        assertThat(new HashSet<String>() {{ add("a"); add("b"); add("c");}}).contains(member);
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
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("a", 1.0);
            put("b", 2.0);
            put("c", 3.0);
        }});
        List<String> members = jedis.zrandmember("myzset", 2);
        assertThat(members.size()).isLessThanOrEqualTo(2);
        assertThat(new HashSet<String>() {{ add("a"); add("b"); add("c");}}).containsAll(members);
    }

    @TestTemplate
    void zrandMemberWithNegativeCountReturnsRepeatedElements(Jedis jedis) {
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("x", 1.0);
            put("y", 2.0);
            put("z", 3.0);
        }});
        List<String> members = jedis.zrandmember("myzset", -10);
        assertThat(members).hasSize(10);
        assertThat(new HashSet<String>() {{ add("x"); add("y"); add("z");}}).containsAll(members);
    }

    @TestTemplate
    void zrandMemberWithCountAndWithScores(Jedis jedis) {
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("apple", 5.0);
            put("banana", 7.0);
            put("carrot", 9.0);
        }});
        List<Tuple> result = jedis.zrandmemberWithScores("myzset", 2);
        assertThat(result.size()).isEqualTo(2);

        assertThat(result.get(0).getScore()).isIn(5.0, 7.0, 9.0);
        assertThat(result.get(1).getScore()).isIn(5.0, 7.0, 9.0);

        assertThat(result.get(0).getElement()).isIn("apple", "banana", "carrot");
        assertThat(result.get(1).getElement()).isIn("apple", "banana", "carrot");
    }

    @TestTemplate
    void zrandMemberReturnsAllWhenCountExceedsSet(Jedis jedis) {
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("1", 1.0);
            put("2", 2.0);
            put("3", 3.0);
        }});
        List<String> members = jedis.zrandmember("myzset", 10);
        assertThat(members).containsExactlyInAnyOrder("1", "2", "3");
    }

    @TestTemplate
    void zrandMemberZeroCountReturnsEmptyList(Jedis jedis) {
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("1", 1.0);
            put("2", 2.0);
        }});
        List<String> result = jedis.zrandmember("myzset", 0);
        assertThat(result).isEmpty();
    }

    @TestTemplate
    void zrandMemberWithInvalidWithScoresThrowsError(Jedis jedis) {
        jedis.zadd("myzset", new HashMap<String, Double>() {{
            put("1", 1.0);
        }});
        try {
            jedis.sendCommand(Protocol.Command.ZRANDMEMBER, "myzset", "1", "WRONGARG");
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("ERR syntax error");
        }
    }

    @TestTemplate
    void zrandMemberWithInvalidArgumentAndNonExistingKeyThrowsError(Jedis jedis) {
        try {
            jedis.sendCommand(Protocol.Command.ZRANDMEMBER, "myzset", "WRONGARG");
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("ERR value is not an integer or out of range");
        }
    }
}
