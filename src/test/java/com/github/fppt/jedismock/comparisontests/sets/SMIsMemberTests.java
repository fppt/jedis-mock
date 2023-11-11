package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.stream.IntStream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class SMIsMemberTests {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenElementsExist_EnsureReturnsTrue(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");

        jedis
                .smismember("set", "a", "b", "c")
                .forEach(Assertions::assertTrue);
    }

    @TestTemplate
    public void whenElementsDoNotExist_EnsureReturnsFalse(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");

        jedis
                .smismember("set", "d", "e", "f")
                .forEach(Assertions::assertFalse);
    }

    @TestTemplate
    public void whenSetDoesNotExist_EnsureReturnsFalse(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");

        jedis
                .smismember("otherSet", "a", "b", "f")
                .forEach(Assertions::assertFalse);
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
                IntStream.range(0, 100_000)
                        .filter(el -> el % 2 == 0)
                        .mapToObj(Integer::toString)
                        .toArray(String[]::new)
        );

        boolean wasAdded = true;

        for (boolean el : jedis.smismember(
                "set",
                IntStream.range(0, 100_000)
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
