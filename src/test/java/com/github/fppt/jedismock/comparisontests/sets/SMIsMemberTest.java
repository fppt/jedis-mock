package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.Arrays;
import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class SMIsMemberTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void simpleCase(Jedis jedis) {
        jedis.sadd("myset", "one");
        Assertions.assertEquals(
                Arrays.asList(true, false),
                jedis.smismember("myset", "one", "notamemeber"));
    }

    @TestTemplate
    public void whenElementsExist_EnsureReturnsTrue(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");
        Assertions.assertEquals(
                Arrays.asList(true, true, true),
                jedis.smismember("set", "a", "b", "c"));
    }

    @TestTemplate
    public void whenElementsDoNotExist_EnsureReturnsFalse(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");
        Assertions.assertEquals(
                Arrays.asList(false, false, false),
                jedis.smismember("set", "d", "e", "f"));
    }

    @TestTemplate
    public void whenSetDoesNotExist_EnsureReturnsFalse(Jedis jedis) {
        Assertions.assertEquals(
                Arrays.asList(false, false, false),
                jedis.smismember("otherSet", "a", "b", "f"));
    }

    @TestTemplate
    public void whenMissingArguments_EnsureThrowsException(Jedis jedis) {
        jedis.sadd("set", "a");
        assertThrows(JedisDataException.class, () -> jedis.smismember("set"));
    }

    @TestTemplate
    public void stressTest(Jedis jedis) {
        jedis.sadd(
                "set",
                IntStream.range(0, 1000)
                        .filter(el -> el % 2 == 0)
                        .mapToObj(Integer::toString)
                        .toArray(String[]::new)
        );

        boolean wasAdded = true;

        for (boolean el : jedis.smismember(
                "set",
                IntStream.range(0, 1000)
                        .mapToObj(Integer::toString)
                        .toArray(String[]::new)
        )) {
            if (wasAdded) {
                assertTrue(el);
            } else {
                assertFalse(el);
            }
            wasAdded = !wasAdded;
        }
    }
}
