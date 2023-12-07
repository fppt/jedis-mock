package com.github.fppt.jedismock.comparisontests.hashes;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.entry;

import static org.assertj.core.api.Assertions.within;

@ExtendWith(ComparisonBase.class)
public class HashOperationsTest {

    private final String HASH = "hash";
    private final String FIELD_1 = "field1";
    private final String VALUE_1 = "value1";
    private final String FIELD_2 = "field2";
    private final String VALUE_2 = "value2";
    private final String FIELD_3 = "field3";
    private final String VALUE_3 = "value3";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenIncrementingSet_ensureValuesAreCorrect(Jedis jedis) {
        String key = "my-set-key-hincr";
        jedis.hset(key, "E", "3.14e1");
        jedis.hset(key, "F", "not-a-number");

        assertThat(jedis.hincrBy(key, "A", 3)).isEqualTo(3);
        assertThat(jedis.hincrByFloat(key, "A", 1.5)).isCloseTo(4.5, within(0.00001));
        assertThat(jedis.hincrByFloat(key, "B", -1.5)).isCloseTo(-1.5, within(0.00001));

        assertThatThrownBy(() -> jedis.hincrBy(key, "F", 1))
                .isInstanceOf(JedisDataException.class);

        assertThatThrownBy(() -> jedis.hincrBy(key, "E", 1))
                .isInstanceOf(JedisDataException.class);

        assertThatThrownBy(() -> jedis.hincrByFloat(key, "F", 1))
                .isInstanceOf(JedisDataException.class);

        assertThat(jedis.hincrByFloat(key, "E", 0.01)).isCloseTo(31.41, within(0.00001));
    }

    @TestTemplate
    public void whenHSettingOnTheSameKeys_EnsureReturnTypeIs1WhenKeysAreNew(Jedis jedis) {
        assertThat(jedis.hset(HASH, FIELD_1, VALUE_1)).isEqualTo(1L);
        assertThat(jedis.hset(HASH, FIELD_1, VALUE_1)).isEqualTo(0L);
    }

    @TestTemplate
    public void whenHSettingAndHGetting_EnsureValuesAreSetAndRetreived(Jedis jedis) {
        String field = "my-field";
        String hash = "my-hash";
        String value = "my-value";

        assertThat(jedis.hget(hash, field)).isNull();
        jedis.hset(hash, field, value);
        assertThat(jedis.hget(hash, field)).isEqualTo(value);
    }

    @TestTemplate
    public void whenHSettingAndHGetting_EnsureValuesAreSetAndExist(Jedis jedis) {
        String field = "my-field";
        String hash = "my-hash";
        String value = "my-value";

        assertThat(jedis.hget(hash, field)).isNull();
        jedis.hset(hash, field, value);
        assertThat(jedis.hexists(hash, field)).isTrue();
    }

    @TestTemplate
    public void whenHGetAll_EnsureAllKeysAndValuesReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        //Check first returns
        Map<String, String> result = jedis.hgetAll(HASH);
        assertThat(result).hasSize(2)
                .contains(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2));

        jedis.hset(HASH, FIELD_3, VALUE_3);

        //Check first returns
        result = jedis.hgetAll(HASH);
        assertThat(result).hasSize(3)
                .contains(entry(FIELD_1, VALUE_1), entry(FIELD_2, VALUE_2), entry(FIELD_3, VALUE_3));

        //Check empty case
        result = jedis.hgetAll("rubbish");
        assertThat(result).isEmpty();
    }

    @TestTemplate
    public void whenHKeys_EnsureAllKeysReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        Set<String> toCompare = new HashSet<>();
        toCompare.add(FIELD_1);
        toCompare.add(FIELD_2);

        Set<String> result = jedis.hkeys(HASH);
        assertThat(result).containsExactlyElementsOf(toCompare);

        toCompare.add(FIELD_3);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        result = jedis.hkeys(HASH);
        assertThat(result).containsExactlyElementsOf(toCompare);
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
        assertThat(result).containsExactlyElementsOf(toCompare);

        toCompare.add(VALUE_3);
        jedis.hset(key, FIELD_3, VALUE_3);

        result = new HashSet<>(jedis.hvals(key));
        assertThat(result).containsExactlyElementsOf(toCompare);
    }

    @TestTemplate
    public void whenHLen_EnsureCorrectLengthReturned(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);

        long result = jedis.hlen(HASH);

        assertThat(result).isEqualTo(2);
    }

    @TestTemplate
    void whenHLenIsCalledOnNonExistingKey_zeroIsReturned(Jedis jedis) {
        Long non_existent = jedis.hlen("non_existent");
        assertThat(non_existent).isEqualTo(0);
    }

    @TestTemplate
    public void whenUsingHMget_EnsureAllValuesReturnedForEachField(Jedis jedis) {
        jedis.hset(HASH, FIELD_1, VALUE_1);
        jedis.hset(HASH, FIELD_2, VALUE_2);
        jedis.hset(HASH, FIELD_3, VALUE_3);

        String FIELD_4 = "field4";
        String FIELD_5 = "field5";
        List<String> result = jedis.hmget(HASH, FIELD_1, FIELD_2, FIELD_5, FIELD_3, FIELD_4);

        assertThat(result).containsExactly(VALUE_1, VALUE_2, null, VALUE_3, null);
    }

    @TestTemplate
    public void whenUsingHMset_EnsureAllValuesAreSetForEachField(Jedis jedis) {
        Map<String, String> map = new HashMap<>();
        map.put(FIELD_1, VALUE_1);
        map.put(FIELD_2, VALUE_2);

        jedis.hmset(HASH, map);
        assertThat(jedis.hget(HASH, FIELD_1)).isEqualTo(VALUE_1);
        assertThat(jedis.hget(HASH, FIELD_2)).isEqualTo(VALUE_2);

        map.put(FIELD_2, VALUE_1);
        jedis.hmset(HASH, map);
        assertThat(jedis.hget(HASH, FIELD_1)).isEqualTo(VALUE_1);
        assertThat(jedis.hget(HASH, FIELD_2)).isEqualTo(VALUE_1);
    }

    @TestTemplate
    public void whenUsingHsetnx_EnsureValueIsOnlyPutIfOtherValueDoesNotExist(Jedis jedis) {
        assertThat(jedis.hget(HASH, FIELD_3)).isNull();
        assertThat(jedis.hsetnx(HASH, FIELD_3, VALUE_1)).isEqualTo(1);
        assertThat(jedis.hget(HASH, FIELD_3)).isEqualTo(VALUE_1);
        assertThat(jedis.hsetnx(HASH, FIELD_3, VALUE_2)).isEqualTo(0);
        assertThat(jedis.hget(HASH, FIELD_3)).isEqualTo(VALUE_1);
    }

    @TestTemplate
    public void whenIncrementingWithHIncrByFloat_ensureValuesAreCorrect(Jedis jedis) {
        jedis.hset("key", "subkey", "0");
        jedis.hincrByFloat("key", "subkey", 1.);
        assertThat(jedis.hget("key", "subkey")).isEqualTo("1");
        jedis.hincrByFloat("key", "subkey", 1.5);
        assertThat(jedis.hget("key", "subkey")).isEqualTo("2.5");
    }

    @TestTemplate
    public void whenIncrementingWithHIncrBy_ensureValuesAreCorrect(Jedis jedis) {
        jedis.hset("key", "subkey", "0");
        jedis.hincrBy("key", "subkey", 1);
        assertThat(jedis.hget("key", "subkey")).isEqualTo("1");
        jedis.hincrBy("key", "subkey", 2);
        assertThat(jedis.hget("key", "subkey")).isEqualTo("3");
    }

    @TestTemplate
    public void whenHIncrementingText_ensureException(Jedis jedis) {
        jedis.hset("key", "subkey", "foo");
        assertThatThrownBy(() -> jedis.hincrBy("key", "subkey", 1))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.hincrByFloat("key", "subkey", 1.5))
                .isInstanceOf(JedisDataException.class);
    }

    @TestTemplate
    void hsetwithMap(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("k1", "v1");
        hash.put("k2", "v2");
        final Long added = jedis.hset("key", hash);

        assertThat(added).isEqualTo(2);

        // identity
        final Long added1 = jedis.hset("key", hash);
        assertThat(added1).isEqualTo(0);

        // update
        hash.put("k2", "v3");
        final Long added2 = jedis.hset("key", hash);
        assertThat(added2).isEqualTo(0);
    }

    @TestTemplate
    void checkTTL(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        jedis.hset("foo", hash);
        jedis.expire("foo", 1000000L);
        assertThat(jedis.ttl("foo")).isNotEqualTo(-1L);
        hash.replace("key1", "2");
        jedis.hset("foo", hash);
        assertThat(jedis.ttl("foo")).isNotEqualTo(-1L);
    }

    @TestTemplate
    void checkGetOperation(Jedis jedis) {
        Map<String, String> hash = new HashMap<>();
        hash.put("key1", "1");
        jedis.hset("foo", hash);
        assertThatThrownBy(() -> jedis.get("foo"))
                .isInstanceOf(JedisDataException.class);
    }

    @TestTemplate
    public void testHsetNonUTF8binary(Jedis jedis) {
        byte[] msg = new byte[]{(byte) 0xbe};
        jedis.hset("foo".getBytes(), "bar".getBytes(), msg);
        byte[] newMsg = jedis.hget("foo".getBytes(), "bar".getBytes());
        assertThat(newMsg).containsExactlyInAnyOrder(msg);
    }

    @TestTemplate
    public void testHsetEmptyString(Jedis jedis) {
        jedis.hset("foo", "bar", "");
        assertThat(jedis.hget("foo", "bar")).isEqualTo("");
    }

    @TestTemplate
    public void testHsetDoesntFailOnLongPayload(Jedis jedis) {
        // 1001 symbol string
        char[] buf = new char[1001];
        Arrays.fill(buf, 'a');
        String value = new String(buf);
        jedis.hset("foo", "bar", value);
        assertThat(jedis.hget("foo", "bar")).isEqualTo(value);
    }


}
