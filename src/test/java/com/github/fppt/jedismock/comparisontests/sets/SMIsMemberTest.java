package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class SMIsMemberTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void simpleCase(Jedis jedis) {
        jedis.sadd("myset", "one");
        assertThat(jedis.smismember("myset", "one", "notamemeber")).containsExactly(true, false);
    }

    @TestTemplate
    public void whenElementsExist_EnsureReturnsTrue(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");
        assertThat(jedis.smismember("set", "a", "b", "c")).containsExactly(true, true, true);
    }

    @TestTemplate
    public void whenElementsDoNotExist_EnsureReturnsFalse(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");
        assertThat(jedis.smismember("set", "d", "e", "f")).containsExactly(false, false, false);
    }

    @TestTemplate
    public void whenSetDoesNotExist_EnsureReturnsFalse(Jedis jedis) {
        assertThat(jedis.smismember("otherSet", "a", "b", "f")).containsExactly(false, false, false);
    }

    @TestTemplate
    public void whenMissingArguments_EnsureThrowsException(Jedis jedis) {
        jedis.sadd("set", "a");
        assertThatThrownBy(() -> jedis.smismember("set"))
                .isInstanceOf(JedisDataException.class);
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
                assertThat(el).isTrue();
            } else {
                assertThat(el).isFalse();
            }
            wasAdded = !wasAdded;
        }
    }
}
