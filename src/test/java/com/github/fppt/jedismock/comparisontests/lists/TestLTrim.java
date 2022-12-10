package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;

import static org.junit.jupiter.api.Assertions.assertEquals;

@ExtendWith(ComparisonBase.class)
public class TestLTrim {

    private final static String key = "`ListKey";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
        jedis.del(key);
        jedis.rpush(key, "e0");
        jedis.rpush(key, "e1");
        jedis.rpush(key, "e2");
    }

    @TestTemplate
    @DisplayName("Keep #0 element")
    public void ltrim0(Jedis jedis) {
        verify(jedis, 0, 0, "e0");
    }

    @TestTemplate
    @DisplayName("Keep #1 element")
    public void ltrim1(Jedis jedis) {
        verify(jedis, 1, 1, "e1");
    }

    @TestTemplate
    @DisplayName("Keep #2 element")
    public void ltrim2(Jedis jedis) {
        verify(jedis, 2, 2, "e2");
    }

    @TestTemplate
    @DisplayName("Keep #0 (-3) element")
    public void ltrimMinus3(Jedis jedis) {
        verify(jedis, -3, -3, "e0");
    }

    @TestTemplate
    @DisplayName("Keep #-2 element")
    public void ltrimMinus2(Jedis jedis) {
        verify(jedis, -2, -2, "e1");
    }

    @TestTemplate
    @DisplayName("Keep #-1 element")
    public void ltrimMinus1(Jedis jedis) {
        verify(jedis, -1, -1, "e2");
    }

    @TestTemplate
    @DisplayName("Keep #0-1 elements")
    public void ltrim0to1(Jedis jedis) {
        verify(jedis, 0, 1, "e0,e1");
    }

    @TestTemplate
    @DisplayName("Keep #1-2 elements")
    public void ltrim1to2(Jedis jedis) {
        verify(jedis, 1, 2, "e1,e2");
    }

    @TestTemplate
    @DisplayName("Keep #0-2 elements")
    public void ltrim0to2(Jedis jedis) {
        verify(jedis, 0, 2, "e0,e1,e2");
    }

    @TestTemplate
    @DisplayName("Keep #0-2(-1) elements")
    public void ltrim0toMinus1(Jedis jedis) {
        verify(jedis, 0, -1, "e0,e1,e2");
    }

    @TestTemplate
    @DisplayName("Keep #1-2(-1) elements")
    public void ltrim1toMinus1(Jedis jedis) {
        verify(jedis, 1, -1, "e1,e2");
    }

    @TestTemplate
    @DisplayName("Keep #2 (2..-1) element")
    public void ltrim2toMinus1(Jedis jedis) {
        verify(jedis, 2, -1, "e2");
    }


    @TestTemplate
    @DisplayName("Keep #0-1 elements")
    public void ltrim0toMinus2(Jedis jedis) {
        verify(jedis, 0, -2, "e0,e1");
    }

    @TestTemplate
    @DisplayName("Keep #1 (1..-2) element")
    public void ltrim1toMinus2(Jedis jedis) {
        verify(jedis, 1, -2, "e1");
    }

    @TestTemplate
    @DisplayName("Remove all elements")
    public void ltrim2toMinus2(Jedis jedis) {
        verify(jedis, 2, -2, "");
    }

    @TestTemplate
    @DisplayName("Keep #0 element")
    public void ltrimMinus4to0(Jedis jedis) {
        verify(jedis, -4, 0, "e0");
    }

    @TestTemplate
    @DisplayName("Keep all elements (-4..-1)")
    public void ltrimMinus4toMinus1(Jedis jedis) {
        verify(jedis, -4, -1, "e0,e1,e2");
    }

    @TestTemplate
    @DisplayName("Keep all elements (-4..5)")
    public void ltrimMinus4to5(Jedis jedis) {
        verify(jedis, -4, 5, "e0,e1,e2");
    }


    @TestTemplate
    @DisplayName("Keep all elements (-5..5)")
    public void ltrimMinus5to5(Jedis jedis) {
        verify(jedis, -5, 5, "e0,e1,e2");
    }

    @TestTemplate
    @DisplayName("Remove all elements (-5..-5)")
    public void ltrimMinus5toMinus5(Jedis jedis) {
        verify(jedis, -5, -5, "");
    }

    private void verify(Jedis jedis, int start, int stop, String expected) {
        String result = jedis.ltrim(key, start, stop);
        assertEquals("OK", result);
        assertEquals(expected, String.join(",", jedis.lrange(key, 0, -1)));
    }

}
