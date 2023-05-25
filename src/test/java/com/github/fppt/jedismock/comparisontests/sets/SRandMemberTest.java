package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
            Assertions.assertTrue(set.contains(member));
            usedElements.add(member);
        }
        Assertions.assertEquals(new HashSet<>(set), usedElements);
    }

    @TestTemplate
    void randMemberReturnsOnlyElementOfTheSet(Jedis jedis) {
        Set<String> set = Collections.singleton("d");
        jedis.sadd("foo", set.toArray(new String[0]));
        for (int i = 0; i < 100; i++) {
            Assertions.assertEquals("d", jedis.srandmember("foo"));
        }
    }

    @TestTemplate
    void randMemberReturnsNullForAnEmptySet(Jedis jedis) {
        for (int i = 0; i < 100; i++) {
            Assertions.assertNull(jedis.srandmember("foo"));
        }
    }

    @TestTemplate
    void randMemberOverNonExistentMustReturnEmptyList(Jedis jedis) {
        //Checking negative, positive and zero range
        for (int i = -3; i < 4; i++) {
            Assertions.assertTrue(jedis.srandmember("foo", i).isEmpty());
        }
    }

    @TestTemplate
    void randMemberReturnsDistinctElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c", "d", "e");
        jedis.sadd("foo", set.toArray(new String[0]));
        for (int i = 0; i < 1000; i++) {
            List<String> members = jedis.srandmember("foo", 3);
            Assertions.assertTrue(set.containsAll(members));
            Assertions.assertEquals(3, new HashSet<>(members).size());
        }
    }

    @TestTemplate
    void randMemberReturnsAllElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c", "d", "e");
        jedis.sadd("foo", set.toArray(new String[0]));
        List<String> members = jedis.srandmember("foo", 10);
        Assertions.assertEquals(new HashSet<>(set), new HashSet<>(members));
    }

    @TestTemplate
    void randMemberReturnsNoElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c", "d", "e");
        jedis.sadd("foo", set.toArray(new String[0]));
        List<String> members = jedis.srandmember("foo", 0);
        Assertions.assertEquals(0, members.size());
    }

    @TestTemplate
    void randMemberReturnsRepeatedElements(Jedis jedis) {
        Collection<String> set = Arrays.asList("a", "b", "c");
        jedis.sadd("foo", set.toArray(new String[0]));
        List<String> members = jedis.srandmember("foo", -100);
        Assertions.assertEquals(100, members.size());
        Assertions.assertTrue(set.containsAll(members));
    }

    @TestTemplate
    void randMemberReturnOneElementAsSingletonList(Jedis jedis) {
        jedis.sadd("key", "a");
        Assertions.assertEquals(Collections.singletonList("a"), jedis.srandmember("key", 1));
        Assertions.assertEquals(Collections.emptyList(), jedis.srandmember("key", 0));
        Assertions.assertEquals(Collections.singletonList("a"), jedis.srandmember("key", -1));
    }
}
