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
public class SInterSInterStoreTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
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
        assertThat(intersection).isEqualTo(expectedIntersection1);

        intersection = jedis.sinter(key1, key2, key3);
        assertThat(intersection).isEqualTo(expectedIntersection2);
    }

    @TestTemplate
    public void whenUsingSInterStore_testIntersectionIsStored(Jedis jedis) {
        String key1 = "set1";
        String key2 = "set2";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        Set<String> mySet2 = new HashSet<>(Arrays.asList("b", "d", "e", "f"));

        Set<String> expectedIntersection = new HashSet<>(Arrays.asList("b", "d"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));

        String destination = "set3";

        Long elementsInIntersection = jedis.sinterstore(destination, key1, key2);
        assertThat(elementsInIntersection).isEqualTo(2);

        assertThat(jedis.smembers(destination)).isEqualTo(expectedIntersection);
    }

    @TestTemplate
    public void deletesDestinationIfResultIsEmpty(Jedis jedis) {
        jedis.sadd("dest", "a", "b");
        jedis.sadd("src", "c", "d");
        jedis.sadd("other", "e", "f");
        assertThat(jedis.exists("dest")).isTrue();
        jedis.sinterstore("dest", "src", "other");
        assertThat(jedis.exists("dest")).isFalse();
    }

}
