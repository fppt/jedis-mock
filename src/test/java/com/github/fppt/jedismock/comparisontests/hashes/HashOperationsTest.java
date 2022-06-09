package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

@ExtendWith(ComparisonBase.class)
public class HashOperationsTest {

    private final String HASH = "hash";
    private final String FIELD_1 = "field1";
    private final String VALUE_1 = "value1";
    private final String FIELD_2 = "field2";
    private final String VALUE_2 = "value2";
    private final String FIELD_3 = "field3";
    private final String VALUE_3 = "value3";
    private final String FIELD_4 = "field4";
    private final String FIELD_5 = "field5";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenIncrementingSet_ensureValuesAreCorrect(Jedis jedis) {
        String key = "my-set-key-hincr";
        jedis.hset(key, "E", "3.14e1");
        jedis.hset(key, "F", "not-a-number");

        assertEquals(3, jedis.hincrBy(key, "A", 3));
        assertEquals(4.5, jedis.hincrByFloat(key, "A", 1.5), 0.00001);
        assertEquals(-1.5, jedis.hincrByFloat(key, "B", -1.5), 0.00001);

        try {
            jedis.hincrBy(key, "F", 1);
            fail("Exception not thrown");
        } catch (JedisDataException ignored) {
            // Non-integer value
        }

        try {
            jedis.hincrBy(key, "E", 1);
            fail("Exception not thrown");
        } catch (JedisDataException ignored) {
            // Non-integer value
        }

        try {
            jedis.hincrByFloat(key, "F", 1);
            fail("Exception not thrown");
        } catch (JedisDataException ignored) {
            // Non-numeric value
        }

        assertEquals(31.41, jedis.hincrByFloat(key, "E", 0.01), 0.00001);
    }

    @TestTemplate
    public void whenHSettingOnTheSameKeys_EnsureReturnTypeIs1WhenKeysAreNew(Jedis jedis) {
        assertEquals(1L, jedis.hset(HASH, FIELD_1, VALUE_1));
        assertEquals(0L, jedis.hset(HASH, FIELD_1, VALUE_1));
    }

    @TestTemplate
    public void whenHSettingAndHGetting_EnsureValuesAreSetAndRetreived(Jedis jedis) {
        String field = "my-field";
        String hash = "my-hash";
        String value = "my-value";

        assertNull(jedis.hget(hash, field));
        jedis.hset(hash, field, value);
        assertEquals(value, jedis.hget(hash, field));
    }

    @TestTemplate
    public void whenHSettingAndHGetting_EnsureValuesAreSetAndExist(Jedis jedis) {
        String field = "my-field";
        String hash = "my-hash";
        String value = "my-value";

        assertNull(jedis.hget(hash, field));
        jedis.hset(hash, field, value);
        assertTrue(jedis.hexists(hash, field));
    }

    @TestTemplate
    public void whenHGetAll_EnsureAllKeysAndValuesReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        //Check first returns
        Map<String, String> result = jedis.hgetAll(HASH);
        assertEquals(2, result.size());
        assertEquals(VALUE_1, result.get(FIELD_1));
        assertEquals(VALUE_2, result.get(FIELD_2));

        jedis.hset(HASH, FIELD_3, VALUE_3);

        //Check first returns
        result = jedis.hgetAll(HASH);
        assertEquals(3, result.size());
        assertEquals(VALUE_1, result.get(FIELD_1));
        assertEquals(VALUE_2, result.get(FIELD_2));
        assertEquals(VALUE_3, result.get(FIELD_3));

        //Check empty case
        result = jedis.hgetAll("rubbish");
        assertEquals(0, result.size());
    }

    @TestTemplate
    public void whenHKeys_EnsureAllKeysReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        Set<String> toCompare = new HashSet<>();
        toCompare.add(FIELD_1);
        toCompare.add(FIELD_2);

        Set<String> result = jedis.hkeys(HASH);
        assertEquals(result, toCompare);

        toCompare.add(FIELD_3);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        result = jedis.hkeys(HASH);
        assertEquals(result, toCompare);
    }

    @TestTemplate
    public void whenHVals_EnsureAllValuesReturned(Jedis jedis) {
        String key = "my-hvals-key";
        jedis.hset(key, FIELD_1, VALUE_1);
        jedis.hset(key, FIELD_2, VALUE_2);

        Set<String> toCompare = new HashSet<>();
        toCompare.add(VALUE_1);
        toCompare.add(VALUE_2);
        Set<String> result = new HashSet<>(jedis.hvals(key));
        assertEquals(result, toCompare);

        toCompare.add(VALUE_3);
        jedis.hset(key, FIELD_3, VALUE_3);

        result = new HashSet<>(jedis.hvals(key));
        assertEquals(result, toCompare);
    }

    @TestTemplate
    public void whenHLen_EnsureCorrectLengthReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        long result = jedis.hlen(HASH);

        assertEquals(2, result);
    }

    @TestTemplate
    void whenHLenIsCalledOnNonExistingKey_zeroIsReturned(Jedis jedis) {
        Long non_existent = jedis.hlen("non_existent");
        assertEquals(0, non_existent);
    }

    @TestTemplate
    public void whenUsingHMget_EnsureAllValuesReturnedForEachField(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        List<String> result = jedis.hmget(HASH, FIELD_1, FIELD_2, FIELD_5, FIELD_3, FIELD_4);

        assertEquals(5, result.size());
        assertEquals(VALUE_1, result.get(0));
        assertEquals(VALUE_2, result.get(1));
        assertNull(result.get(2));
        assertEquals(VALUE_3, result.get(3));
        assertNull(result.get(4));
    }

    @TestTemplate
    public void whenUsingHMset_EnsureAllValuesAreSetForEachField(Jedis jedis) {
        Map<String, String> map = new HashMap<>();
        map.put(FIELD_1, VALUE_1);
        map.put(FIELD_2, VALUE_2);

        jedis.hmset(HASH, map);
        assertEquals(VALUE_1, jedis.hget(HASH, FIELD_1));
        assertEquals(VALUE_2, jedis.hget(HASH, FIELD_2));

        map.put(FIELD_2, VALUE_1);
        jedis.hmset(HASH, map);
        assertEquals(VALUE_1, jedis.hget(HASH, FIELD_1));
        assertEquals(VALUE_1, jedis.hget(HASH, FIELD_2));
    }

    @TestTemplate
    public void whenUsingHsetnx_EnsureValueIsOnlyPutIfOtherValueDoesNotExist(Jedis jedis) {
        assertNull(jedis.hget(HASH, FIELD_3));
        assertEquals(1, jedis.hsetnx(HASH, FIELD_3, VALUE_1));
        assertEquals(VALUE_1, jedis.hget(HASH, FIELD_3));
        assertEquals(0, jedis.hsetnx(HASH, FIELD_3, VALUE_2));
        assertEquals(VALUE_1, jedis.hget(HASH, FIELD_3));
    }

    @TestTemplate
    public void whenIncrementingWithHIncrByFloat_ensureValuesAreCorrect(Jedis jedis) {
        jedis.hset("key", "subkey", "0");
        jedis.hincrByFloat("key", "subkey", 1.);
        assertEquals("1", jedis.hget("key", "subkey"));
        jedis.hincrByFloat("key", "subkey", 1.5);
        assertEquals("2.5", jedis.hget("key", "subkey"));
    }

    @TestTemplate
    public void whenIncrementingWithHIncrBy_ensureValuesAreCorrect(Jedis jedis) {
        jedis.hset("key", "subkey", "0");
        jedis.hincrBy("key", "subkey", 1);
        assertEquals("1", jedis.hget("key", "subkey"));
        jedis.hincrBy("key", "subkey", 2);
        assertEquals("3", jedis.hget("key", "subkey"));
    }

    @TestTemplate
    public void whenHIncrementingText_ensureException(Jedis jedis) {
        jedis.hset("key", "subkey", "foo");
        assertThrows(JedisDataException.class, () -> jedis.hincrBy("key", "subkey", 1));
        assertThrows(JedisDataException.class, () -> jedis.hincrByFloat("key", "subkey", 1.5));
    }

    @TestTemplate
    void hsetwithMap(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("k1", "v1");
        hash.put("k2", "v2");
        final Long added = jedis.hset("key", hash);

        assertEquals(2, added);

        // identity
        final Long added1 = jedis.hset("key", hash);
        assertEquals(0, added1);

        // update
        hash.put("k2", "v3");
        final Long added2 = jedis.hset("key", hash);
        assertEquals(0, added2);
    }

    @TestTemplate
    void checkTTL(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        jedis.hset("foo", hash);
        jedis.expire("foo", 1000000L);
        assertNotEquals(-1L, jedis.ttl("foo"));
        hash.replace("key1", "2");
        jedis.hset("foo", hash);
        assertNotEquals(-1L, jedis.ttl("foo"));
    }

    @TestTemplate
    void checkGetOperation(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        jedis.hset("foo", hash);
        assertThrows(JedisDataException.class, () -> jedis.get("foo"));
    }

    @TestTemplate
    public void testHsetNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.hset("foo".getBytes(), "bar".getBytes(), msg);
        byte[] newMsg = jedis.hget("foo".getBytes(), "bar".getBytes());
        assertArrayEquals(msg, newMsg);
    }
}
