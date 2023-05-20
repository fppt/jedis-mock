package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
class ScriptTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void loadTest(Jedis jedis) {
        String sha = jedis.scriptLoad("return 'Hello'");
        assertEquals(sha, sha.toLowerCase());
        assertEquals("Hello", jedis.evalsha(sha));
        assertTrue(jedis.scriptExists(sha));
    }

    @TestTemplate
    public void loadParametrizedTest(Jedis jedis) {
        String sha = jedis.scriptLoad("return ARGV[1]");
        String supposedReturn = "Hello, scripting!";
        Object response = jedis.evalsha(sha, 0, supposedReturn);
        assertEquals(String.class, response.getClass());
        assertEquals(supposedReturn, response);
        assertTrue(jedis.scriptExists(sha));
    }

    @TestTemplate
    public void scriptFlushRemovesScripts(Jedis jedis) {
        String s1 = jedis.scriptLoad("return 1");
        String s2 = jedis.scriptLoad("return 2");
        assertEquals(Arrays.asList(true, true), jedis.scriptExists(s1, s2));
        jedis.scriptFlush();
        assertEquals(Arrays.asList(false, false), jedis.scriptExists(s1, s2));
    }
}
