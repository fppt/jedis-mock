package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class SRandMemberTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    void randMemberReturnsARandomElementOfTheSet(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c");
        jedis.sadd("foo", set.toArray(new String[0]));
        Set<String> usedElements = new HashSet<>();
        for (int i = 0; i < 5000; i++) {
            String member = jedis.srandmember("foo");
            assertThat(set).contains(member);
            usedElements.add(member);
        }
        assertThat(usedElements).containsExactlyElementsOf(set);
    }

    @TestTemplate
    void randMemberReturnsOnlyElementOfTheSet(Jedis jedis) {
        Set<String> set = Collections.singleton("d");
        jedis.sadd("foo", set.toArray(new String[0]));
        for (int i = 0; i < 100; i++) {
            assertThat(jedis.srandmember("foo")).isEqualTo("d");
        }
    }

    @TestTemplate
    void randMemberReturnsNullForAnEmptySet(Jedis jedis) {
        for (int i = 0; i < 100; i++) {
            assertThat(jedis.srandmember("foo")).isNull();
        }
    }

    @TestTemplate
    void randMemberOverNonExistentMustReturnEmptyList(Jedis jedis) {
        //Checking negative, positive and zero range
        for (int i = -3; i < 4; i++) {
            assertThat(jedis.srandmember("foo", i)).isEmpty();
        }
    }

    @TestTemplate
    void randMemberReturnsDistinctElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c", "d", "e");
        jedis.sadd("foo", set.toArray(new String[0]));
        for (int i = 0; i < 1000; i++) {
            List<String> members = jedis.srandmember("foo", 3);
            assertThat(set).containsAll(members);
            assertThat(new HashSet<>(members)).hasSize(3);
        }
    }

    @TestTemplate
    void randMemberReturnsAllElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c", "d", "e");
        jedis.sadd("foo", set.toArray(new String[0]));
        List<String> members = jedis.srandmember("foo", 10);
        assertThat(members).containsExactlyInAnyOrderElementsOf(set);
    }

    @TestTemplate
    void randMemberReturnsNoElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c", "d", "e");
        jedis.sadd("foo", set.toArray(new String[0]));
        List<String> members = jedis.srandmember("foo", 0);
        assertThat(members).isEmpty();
    }

    @TestTemplate
    void randMemberReturnsRepeatedElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c");
        jedis.sadd("foo", set.toArray(new String[0]));
        List<String> members = jedis.srandmember("foo", -100);
        assertThat(members).hasSize(100);
        assertThat(set).containsAll(members);
    }

    @TestTemplate
    void randMemberReturnOneElementAsSingletonList(Jedis jedis) {
        jedis.sadd("key", "a");
        assertThat(jedis.srandmember("key", 1)).containsExactly("a");
        assertThat(jedis.srandmember("key", 0)).isEmpty();
        assertThat(jedis.srandmember("key", -1)).containsExactly("a");
    }

    @TestTemplate
    void randMemberWithInvalidArgumentAndNonExistingKeyThrowsError(Jedis jedis) {
        try {
            jedis.sendCommand(Protocol.Command.SRANDMEMBER, "myzset", "WRONGARG");
        } catch (Exception e) {
            assertThat(e.getMessage()).contains("ERR value is not an integer or out of range");
        }
    }
}
