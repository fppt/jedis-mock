package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(jedis.smembers(key)).isEqualTo(mySet);
    }

    @TestTemplate
    public void whenDuplicateValuesAddedToSet_ReturnsAddedValuesCountOnly(Jedis jedis) {
        String key = "my-set-key-sadd";
        assertThat(jedis.sadd(key, "A", "B", "C", "B")).isEqualTo(3);
        assertThat(jedis.sadd(key, "A", "C", "E", "B")).isEqualTo(1);
    }

    @TestTemplate
    public void whenAddingToASet_ensureCountIsUpdated(Jedis jedis) {
        String key = "my-counted-set-key";
        Set<String> mySet = new HashSet<>(Arrays.asList("d", "e", "f"));

        //Add everything from the set
        mySet.forEach(value -> jedis.sadd(key, value));

        //Get it all back
        assertThat(jedis.scard(key)).isEqualTo(mySet.size());
    }

    @TestTemplate
    public void whenCalledForNonExistentSet_ensureScardReturnsZero(Jedis jedis) {
        String key = "non-existent";
        assertThat(jedis.scard(key)).isEqualTo(0);
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
        assertThat(jedis.smembers(key)).isEqualTo(mySet);
        assertThat(removed).isEqualTo(2);
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
                assertThat(mySet).contains(poppedValue);
            }
        } while (poppedValue != null);
    }

    @TestTemplate
    public void poppingManyKeys(Jedis jedis) {
        String key = "my-set-key-spop";
        jedis.sadd(key, "a", "b", "c", "d");
        assertThat(jedis.spop(key, 3)).hasSize(3);
        assertThat(jedis.scard(key)).isEqualTo(1);
    }

    @TestTemplate
    public void poppingZeroAndOneKey(Jedis jedis) {
        String key = "key-pop";
        jedis.sadd(key, "a");
        assertThat(jedis.exists(key)).isTrue();
        assertThat(jedis.spop(key, 1)).containsExactly("a");
        assertThat(jedis.exists(key)).isFalse();
        assertThat(jedis.spop(key, 0)).isEmpty();
    }

    @TestTemplate
    public void poppingNonExistentSet(Jedis jedis) {
        String key = "non-existent";
        assertThat(jedis.spop(key, 1)).isEmpty();
        assertThat(jedis.spop(key)).isNull();
    }

    @TestTemplate
    public void ensureSismemberReturnsCorrectValues(Jedis jedis) {
        String key = "my-set-key-sismember";
        jedis.sadd(key, "A", "B");
        assertThat(jedis.sismember(key, "A")).isTrue();
        assertThat(jedis.sismember(key, "C")).isFalse();
        assertThat(jedis.sismember(key + "-nonexistent", "A")).isFalse();
    }


    @TestTemplate
    public void testFailingGetOperation(Jedis jedis) {
        jedis.sadd("my-set-key", "a", "b", "c", "d");
        assertThatThrownBy(() -> jedis.get("my-set-key"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
    }

    @TestTemplate
    public void testSaddNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.sadd("foo".getBytes(), msg);
        byte[] newMsg = jedis.spop("foo".getBytes());
        assertThat(newMsg).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testSMoveExistingElement(Jedis jedis) {
        jedis.sadd("myset", "one", "two");
        jedis.sadd("myotherset", "three");
        assertThat(jedis.smove("myset", "myotherset", "two")).isEqualTo(1);
        assertThat(jedis.smembers("myset")).containsExactly("one");
        assertThat(jedis.smembers("myotherset")).containsExactlyInAnyOrder("two", "three");
    }

    @TestTemplate
    public void testSMoveNonExistingElement(Jedis jedis) {
        jedis.sadd("myset", "one", "two");
        jedis.sadd("myotherset", "three");
        assertThat(jedis.smove("myset", "myotherset", "four")).isEqualTo(0);
        assertThat(jedis.smembers("myset")).containsExactlyInAnyOrder("one", "two");
        assertThat(jedis.smembers("myotherset")).containsExactly("three");
    }

    @TestTemplate
    public void testSMoveWrongTypesSrcDest(Jedis jedis) {

        String key1 = "key1";
        String key2 = "key2";
        
        jedis.set(key1, "a");
        jedis.sadd(key2, "b");

        assertThatThrownBy(() -> jedis.smove(key1, key2, "a"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
        assertThatThrownBy(() -> jedis.smove(key2, key1, "a"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
    }
}
