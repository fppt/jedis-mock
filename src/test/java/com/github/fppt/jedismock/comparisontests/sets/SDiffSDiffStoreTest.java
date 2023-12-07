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
public class SDiffSDiffStoreTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void sDiffTwoSetsTest(Jedis jedis) {
        String key1 = "set1";
        String key2 = "set2";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        Set<String> mySet2 = new HashSet<>(Arrays.asList("c", "d", "e"));

        Set<String> expectedDifference = new HashSet<>(Arrays.asList("a", "b"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));


        Set<String> result = jedis.sdiff(key1, key2);
        assertThat(result).containsExactlyElementsOf(expectedDifference);
    }

    @TestTemplate
    public void sDiffThreeSetsTest(Jedis jedis) {
        String key1 = "set1";
        String key2 = "set2";
        String key3 = "set3";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        Set<String> mySet2 = new HashSet<>(Collections.singletonList("c"));
        Set<String> mySet3 = new HashSet<>(Arrays.asList("a", "c", "e"));

        Set<String> expectedDifference = new HashSet<>(Arrays.asList("b", "d"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));
        mySet3.forEach(value -> jedis.sadd(key3, value));


        Set<String> result = jedis.sdiff(key1, key2, key3);
        assertThat(result).containsExactlyElementsOf(expectedDifference);
    }

    @TestTemplate
    public void sDiffStoreTest(Jedis jedis) {
        String key1 = "set1";
        String key2 = "set2";
        Set<String> mySet1 = new HashSet<>(Arrays.asList("a", "b", "c", "d"));
        Set<String> mySet2 = new HashSet<>(Arrays.asList("c", "d", "e"));

        Set<String> expectedDifference = new HashSet<>(Arrays.asList("a", "b"));

        //Add everything from the sets
        mySet1.forEach(value -> jedis.sadd(key1, value));
        mySet2.forEach(value -> jedis.sadd(key2, value));

        String destination = "set3";

        Long elementsInDifference = jedis.sdiffstore(destination, key1, key2);
        assertThat(elementsInDifference).isEqualTo(2);

        assertThat(jedis.smembers(destination)).isEqualTo(expectedDifference);
    }


    @TestTemplate
    public void deletesDestinationIfResultIsEmpty(Jedis jedis) {
        jedis.sadd("dest", "a", "b");
        jedis.sadd("src", "c");
        jedis.sadd("other", "c", "d");
        assertThat(jedis.exists("dest")).isTrue();
        jedis.sdiffstore("dest", "src", "other");
        assertThat(jedis.exists("dest")).isFalse();
    }
}
