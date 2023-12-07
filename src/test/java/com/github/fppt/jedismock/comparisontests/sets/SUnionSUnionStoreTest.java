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

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class SUnionSUnionStoreTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void sUnionTest(Jedis jedis) {
        String key1 = "set1";
        String key2 = "set2";
        String key3 = "set3";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        Set<String> mySet2 = new HashSet<>(Collections.singletonList("c"));
        Set<String> mySet3 = new HashSet<>(Arrays.asList("a", "c", "e", "f"));

        Set<String> expectedUnion = new HashSet<>(Arrays.asList("a", "b", "c", "d", "e", "f"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));
        mySet3.forEach(value -> jedis.sadd(key3, value));


        Set<String> result = jedis.sunion(key1, key2, key3);
        assertThat(result).containsExactlyElementsOf(expectedUnion);
    }

    @TestTemplate
    public void sUnionStoreTest(Jedis jedis) {
        String key1 = "set1";
        String key2 = "set2";
        String key3 = "set3";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        Set<String> mySet2 = new HashSet<>(Collections.singletonList("c"));
        Set<String> mySet3 = new HashSet<>(Arrays.asList("a", "c", "e", "f"));

        Set<String> expectedUnion = new HashSet<>(Arrays.asList("a", "b", "c", "d", "e", "f"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));
        mySet3.forEach(value -> jedis.sadd(key3, value));

        String destination = "set3";

        Long elementsInUnion = jedis.sunionstore(destination, key1, key2, key3);
        assertThat(elementsInUnion).isEqualTo(6);

        assertThat(jedis.smembers(destination)).isEqualTo(expectedUnion);
    }

    @TestTemplate
    public void deletesDestinationIfResultIsEmpty(Jedis jedis) {
        jedis.sadd("dest", "a", "b");
        assertThat(jedis.exists("dest")).isTrue();
        jedis.sinterstore("dest", "src", "other");
        assertThat(jedis.exists("dest")).isFalse();
    }
}
