package com.github.fppt.jedismock.comparisontests.scripting;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@ExtendWith(ComparisonBase.class)
public class EvalTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void evalTest(Jedis jedis) {
        jedis.eval("return 'Hello, scripting!'",0);
    }

    @TestTemplate
    public void evalParametrizedTest(Jedis jedis) {
        jedis.eval("return ARGV[1]",0, "Hello");
    }

    @TestTemplate
    public void evalParametrizedReturnMultipleKeysArgsTest(Jedis jedis) {
        jedis.eval("return { KEYS[1], KEYS[2], ARGV[1], ARGV[2], ARGV[3] }",2, "key1", "key2", "arg1", "arg2", "arg3");
    }

    @TestTemplate
    public void evalRedisEchoTest(Jedis jedis) {
        jedis.eval("return redis.call('ECHO', 'echo')", 0);
    }

}
