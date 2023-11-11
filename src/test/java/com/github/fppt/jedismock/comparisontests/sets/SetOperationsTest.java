package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

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
    public void poppingManyKeys(Jedis jedis) {
        String key = "my-set-key-spop";
        jedis.sadd(key, "a", "b", "c", "d");
        assertEquals(3,
                jedis.spop(key, 3).size());
        assertEquals(1, jedis.scard(key));
    }

    @TestTemplate
    public void poppingZeroAndOneKey(Jedis jedis) {
        String key = "key-pop";
        jedis.sadd(key, "a");
        assertTrue(jedis.exists(key));
        assertEquals(Collections.singleton("a"), jedis.spop(key, 1));
        assertFalse(jedis.exists(key));
        assertEquals(0, jedis.spop(key, 0).size());
    }

    @TestTemplate
    public void poppingNonExistentSet(Jedis jedis) {
        String key = "non-existent";
        assertTrue(jedis.spop(key, 1).isEmpty());
        assertNull(jedis.spop(key));
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
    public void testFailingGetOperation(Jedis jedis) {
        jedis.sadd("my-set-key", "a", "b", "c", "d");
        assertTrue(
                assertThrows(JedisDataException.class, () -> jedis.get("my-set-key"))
                        .getMessage().startsWith("WRONGTYPE"));
    }

    @TestTemplate
    public void testSaddNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.sadd("foo".getBytes(), msg);
        byte[] newMsg = jedis.spop("foo".getBytes());
        assertArrayEquals(msg, newMsg);
    }

    @TestTemplate
    public void testSMoveExistingElement(Jedis jedis) {
        jedis.sadd("myset", "one", "two");
        jedis.sadd("myotherset", "three");
        assertEquals(1, jedis.smove("myset", "myotherset", "two"));
        assertEquals(Collections.singleton("one"), jedis.smembers("myset"));
        assertEquals(new HashSet<>(Arrays.asList("two", "three")),
                jedis.smembers("myotherset"));
    }

    @TestTemplate
    public void testSMoveNonExistingElement(Jedis jedis) {
        jedis.sadd("myset", "one", "two");
        jedis.sadd("myotherset", "three");
        assertEquals(0, jedis.smove("myset", "myotherset", "four"));
        assertEquals(new HashSet<>(Arrays.asList("one", "two")),
                jedis.smembers("myset"));
        assertEquals(Collections.singleton("three"),
                jedis.smembers("myotherset"));
    }

    @TestTemplate
    public void testSMoveWrongTypesSrcDest(Jedis jedis) {

        String key1 = "key1";
        String key2 = "key2";
        
        jedis.set(key1, "a");
        jedis.sadd(key2, "b");

        assertTrue(
                assertThrows(JedisDataException.class, () -> jedis.smove(key1, key2, "a"))
                        .getMessage().startsWith("WRONGTYPE"));
        assertTrue(
                assertThrows(JedisDataException.class, () -> jedis.smove(key2, key1, "a"))
                        .getMessage().startsWith("WRONGTYPE"));
    }
}
