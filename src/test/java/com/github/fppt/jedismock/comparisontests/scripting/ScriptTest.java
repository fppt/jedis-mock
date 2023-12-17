package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
class ScriptTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void loadTest(Jedis jedis) {
        String sha = jedis.scriptLoad("return 'Hello'");
        assertThat(sha.toLowerCase()).isEqualTo(sha);
        assertThat(jedis.evalsha(sha)).isEqualTo("Hello");
        assertThat(jedis.scriptExists(sha)).isTrue();
    }

    @TestTemplate
    public void loadParametrizedTest(Jedis jedis) {
        String sha = jedis.scriptLoad("return ARGV[1]");
        String supposedReturn = "Hello, scripting!";
        Object response = jedis.evalsha(sha, 0, supposedReturn);
        assertThat(response).isInstanceOf(String.class).isEqualTo(supposedReturn);
        assertThat(jedis.scriptExists(sha)).isTrue();
    }

    @TestTemplate
    public void scriptFlushRemovesScripts(Jedis jedis) {
        String s1 = jedis.scriptLoad("return 1");
        String s2 = jedis.scriptLoad("return 2");
        assertThat(jedis.scriptExists(s1, s2)).containsExactly(true, true);
        jedis.scriptFlush();
        assertThat(jedis.scriptExists(s1, s2)).containsExactly(false, false);
    }
}
