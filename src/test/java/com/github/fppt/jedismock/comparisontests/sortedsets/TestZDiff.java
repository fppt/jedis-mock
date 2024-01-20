package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.resps.Tuple;

import java.util.Map;
import java.util.List;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestZDiff {

    private static final String ZSET_KEY_1 = "myzset";
    private static final String ZSET_KEY_2 = "ztmp";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testZDiffNotExistKeyToNotExistDest(Jedis jedis) {
        assertThat(jedis.zdiff(ZSET_KEY_1)).isEmpty();
    }

    @TestTemplate
    public void testZDiffWithEmptySet(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "a");
        jedis.zadd(ZSET_KEY_1, 2, "b");
        List<Tuple> results = jedis.zdiffWithScores(ZSET_KEY_1, ZSET_KEY_2);
        assertThat(results).containsExactly(new Tuple("a", 1.0), new Tuple("b", 2.0));
    }

    @TestTemplate
    public void testZDiffBase(Jedis jedis) {
        jedis.zadd(ZSET_KEY_1, 1, "1");
        jedis.zadd(ZSET_KEY_1, 2, "2");
        jedis.zadd(ZSET_KEY_1, 3, "3");
        jedis.zadd(ZSET_KEY_2, 1, "1");
        jedis.zadd(ZSET_KEY_2, 3, "3");
        jedis.zadd(ZSET_KEY_2, 4, "4");
        List<Tuple> results = jedis.zdiffWithScores(ZSET_KEY_1, ZSET_KEY_2);
        assertThat(results).containsExactly(new Tuple("2", 2.0));
    }

    public static String randomValue(int n) {
        String result = "";
        switch (n) {
            case 0:
                result = String.valueOf(randomSignedInt(1000));
                break;
            case 1:
                result = String.valueOf(randomSignedInt(2_000_000_000));
                break;
            case 2:
                result = String.valueOf(randomSignedInt(4_000_000_000L));
                break;
            case 3:
                result = String.valueOf(randomSignedInt(1_000_000_000_000L));
                break;
            case 4:
                result = randstring(1, 256, "alpha");
                break;
            case 5:
                result = randstring(0, 256, "compr");
                break;
            case 6:
                result = randstring(0, 256, "binary");
                break;
        }
        return result;
    }

    public static long randomSignedInt(long max) {
        long i = ThreadLocalRandom.current().nextLong(max);
        if (ThreadLocalRandom.current().nextDouble() > 0.5) {
            i = -i;
        }
        return i;
    }

    public static int randomInt(int max) {
        return ThreadLocalRandom.current().nextInt(max);
    }

    public static String randstring(int min, int max, String type) {
        int len = min + ThreadLocalRandom.current().nextInt(max - min + 1);
        StringBuilder output = new StringBuilder();
        int minval = 0;
        int maxval = 0;
        switch (type) {
            case "binary":
                maxval = 255;
                break;
            case "alpha":
                minval = 48;
                maxval = 122;
                break;
            case "compr":
                minval = 48;
                maxval = 52;
                break;
        }
        while (len > 0) {
            int num = minval + ThreadLocalRandom.current().nextInt(maxval - minval + 1);
            char rr = (char) num;
            if (type.equals("alpha") && num == 92) {
                continue; // avoid putting '\' char in the string, it can mess up TCL processing
            }
            output.append(rr);
            len--;
        }
        return output.toString();
    }

    @TestTemplate
    public void testZDiffStress(Jedis jedis) {
        for (int j = 0; j < 100; j++) {
            Map<String, String> s = new HashMap<>();
            List<String> argsList = new ArrayList<>();
            int numSets = randomInt(10) + 1;
            for (int i = 0; i < numSets; i++) {
                int numElements = randomInt(100);
                jedis.del("zset_" + i + "{t}");
                argsList.add("zset_" + i + "{t}");
                while (numElements > 0) {
                    String ele = randomValue(randomInt(7));
                    jedis.zadd("zset_" + i + "{t}", randomInt(100), ele);
                    if (i == 0) {
                        s.put(ele, "x");
                    } else {
                        s.remove(ele);
                    }
                    numElements--;
                }
            }
            List<String> result = jedis.zdiff(argsList.toArray(new String[0]));
            assertThat(result).containsExactlyInAnyOrderElementsOf(s.keySet());
        }
    }
}
