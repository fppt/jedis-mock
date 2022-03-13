package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class SetOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenAddingToASet_EnsureTheSetIsUpdated(Jedis jedis) {
        String key = "my-set-key";
        Set<String> mySet = new HashSet<>(Arrays.asList("a", "b", "c", "d"));

        //Add everything from the set
        mySet.forEach(value -> jedis.sadd(key, value));

        //Get it all back
        assertEquals(mySet, jedis.smembers(key));
    }

    @TestTemplate
    public void whenDuplicateValuesAddedToSet_ReturnsAddedValuesCountOnly(Jedis jedis) {
        String key = "my-set-key-sadd";
        assertEquals(3, jedis.sadd(key, "A", "B", "C", "B"));
        assertEquals(1, jedis.sadd(key, "A", "C", "E", "B"));
    }

    @TestTemplate
    public void whenAddingToASet_ensureCountIsUpdated(Jedis jedis) {
        String key = "my-counted-set-key";
        Set<String> mySet = new HashSet<>(Arrays.asList("d", "e", "f"));

        //Add everything from the set
        mySet.forEach(value -> jedis.sadd(key, value));

        //Get it all back
        assertEquals(mySet.size(), jedis.scard(key));
    }

    @TestTemplate
    public void whenCalledForNonExistentSet_ensureScardReturnsZero(Jedis jedis) {
        String key = "non-existent";
        assertEquals(0, jedis.scard(key));
    }

    @TestTemplate
    public void whenRemovingFromASet_EnsureTheSetIsUpdated(Jedis jedis) {
        String key = "my-set-key";
        Set<String> mySet = new HashSet<>(Arrays.asList("a", "b", "c", "d"));

        //Add everything from the set
        mySet.forEach(value -> jedis.sadd(key, value));

        // Remove an element
        mySet.remove("c");
        mySet.remove("d");
        mySet.remove("f");
        long removed = jedis.srem(key, "c", "d", "f");

        //Get it all back
        assertEquals(mySet, jedis.smembers(key));
        assertEquals(2, removed);
    }

    @TestTemplate
    public void whenPoppingFromASet_EnsureTheSetIsUpdated(Jedis jedis) {
        String key = "my-set-key-spop";
        Set<String> mySet = new HashSet<>(Arrays.asList("a", "b", "c", "d"));

        //Add everything from the set
        mySet.forEach(value -> jedis.sadd(key, value));

        String poppedValue;
        do {
            poppedValue = jedis.spop(key);
            if (poppedValue != null) {
                assertTrue(mySet.contains(poppedValue), "Popped value not in set");
            }
        } while (poppedValue != null);
    }

    @TestTemplate
    public void ensureSismemberReturnsCorrectValues(Jedis jedis) {
        String key = "my-set-key-sismember";
        jedis.sadd(key, "A", "B");
        assertTrue(jedis.sismember(key, "A"));
        assertFalse(jedis.sismember(key, "C"));
        assertFalse(jedis.sismember(key + "-nonexistent", "A"));
    }

    @TestTemplate
    public void whenUsingHsinter_EnsureSetIntersectionIsReturned(Jedis jedis) {
        String key1 = "my-set-key-1";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        String key2 = "my-set-key-2";
        Set<String> mySet2 = new HashSet<>(Arrays.asList("b", "d", "e", "f"));
        String key3 = "my-set-key-3";
        Set<String> mySet3 = new HashSet<>(Arrays.asList("b", "e", "f"));

        Set<String> expectedIntersection1 = new HashSet<>(Arrays.asList("b", "d"));
        Set<String> expectedIntersection2 = new HashSet<>(Collections.singletonList("b"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));
        mySet3.forEach(value -> jedis.sadd(key3, value));

        Set<String> intersection = jedis.sinter(key1, key2);
        assertEquals(expectedIntersection1, intersection);

        intersection = jedis.sinter(key1, key2, key3);
        assertEquals(expectedIntersection2, intersection);
    }
}
