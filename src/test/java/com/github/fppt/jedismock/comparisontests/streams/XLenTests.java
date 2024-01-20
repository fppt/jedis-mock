package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;

import java.util.Collections;
import java.util.Random;
import java.util.StringJoiner;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class XLenTests {
    Random r;
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
        r = new Random();
    }

    String getRandomString(int length) {
        StringJoiner joiner = new StringJoiner("");

        for (int i = 0; i < length; ++i) {
            joiner.add(String.valueOf((char)(r.nextInt(26) + 'a')));
        }

        return joiner.toString();
    }

    @TestTemplate
    void whenStreamDoesNotExist_ensureReturnsZero(Jedis jedis) {
        for (int i = 0; i < 100; ++i) {
            String key = getRandomString(r.nextInt(21));
            assertThat(jedis.exists(key)).isFalse();
            assertThat(jedis.xlen(key)).isEqualTo(0);
        }
    }

    @TestTemplate
    void whenStreamExists_ensureReturnsActualSize(Jedis jedis) {
        for (int i = 0; i < 100; ++i) {
            String key = getRandomString(r.nextInt(21));
            int len = r.nextInt(100);

            for (int j = 0; j < len; ++j) {
                jedis.xadd(
                        key,
                        StreamEntryID.NEW_ENTRY,
                        Collections.singletonMap(getRandomString(3), getRandomString(3))
                );
            }

            assertThat(jedis.xlen(key)).isEqualTo(len);

            jedis.del(key);
        }
    }
}
