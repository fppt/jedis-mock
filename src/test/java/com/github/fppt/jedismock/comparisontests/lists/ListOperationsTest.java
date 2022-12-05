package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class ListOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenUsingRpop_EnsureTheLastElementPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertEquals(jedis.rpop(key), "3");
    }

    @TestTemplate
    public void whenUsingLpop_EnsureTheFirstElementPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertEquals("1", jedis.lpop(key));
    }

    @TestTemplate
    public void whenUsingLpopCount_EnsureAllElementsPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3", "4");
        assertEquals(asList("1", "2", "3"), jedis.lpop(key, 3));
        assertEquals(singletonList("4"), jedis.lpop(key, 5));
        assertNull(jedis.lpop(key, 5));
    }

    @TestTemplate
    public void whenUsingRpop_EnsureTheFirstElementPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertEquals("3", jedis.rpop(key));
    }

    @TestTemplate
    public void whenUsingRpopCount_EnsureAllElementsPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3", "4");
        assertEquals(asList("4", "3", "2"), jedis.rpop(key, 3));
        assertEquals(singletonList("1"), jedis.rpop(key, 5));
        assertNull(jedis.rpop(key, 5));
    }

    @TestTemplate
    public void whenUsingRpoplpush_CorrectResultsAreReturned(Jedis jedis) {
        String list1key = "list 1";
        String list2key = "list 2";

        String nullResult = jedis.rpoplpush(list1key, list2key);
        assertNull(nullResult);

        jedis.rpush(list1key, "1", "2", "3");
        jedis.rpush(list2key, "a", "b", "c");

        //Check the lists are in order
        List<String> results1 = jedis.lrange(list1key, 0, -1);
        List<String> results2 = jedis.lrange(list2key, 0, -1);

        assertTrue(results1.contains("1"));
        assertTrue(results1.contains("2"));
        assertTrue(results1.contains("3"));

        assertTrue(results2.contains("a"));
        assertTrue(results2.contains("b"));
        assertTrue(results2.contains("c"));

        //Check that the one list has been pushed into the other
        String result = jedis.rpoplpush(list1key, list2key);
        assertEquals("3", result);

        results1 = jedis.lrange(list1key, 0, -1);
        results2 = jedis.lrange(list2key, 0, -1);

        assertTrue(results1.contains("1"));
        assertTrue(results1.contains("2"));
        assertFalse(results1.contains("3"));

        assertTrue(results2.contains("3"));
        assertTrue(results2.contains("a"));
        assertTrue(results2.contains("b"));
        assertTrue(results2.contains("c"));
    }

    @TestTemplate
    public void whenUsingLrem_EnsureDeletionsWorkAsExpected(Jedis jedis) {
        String key = "my-super-special-sexy-key";
        String hello = "hello";
        String foo = "foo";

        jedis.rpush(key, hello);
        jedis.rpush(key, hello);
        jedis.rpush(key, foo);
        jedis.rpush(key, hello);

        //Everything in order
        List<String> list = jedis.lrange(key, 0, -1);
        assertEquals(hello, list.get(0));
        assertEquals(hello, list.get(1));
        assertEquals(foo, list.get(2));
        assertEquals(hello, list.get(3));

        long numRemoved = jedis.lrem(key, -2, hello);
        assertEquals(2L, numRemoved);

        //Check order again
        list = jedis.lrange(key, 0, -1);
        assertEquals(hello, list.get(0));
        assertEquals(foo, list.get(1));
    }

    @TestTemplate
    public void whenUsingLtrim_EnsureDeletionsWorkAsExpected(Jedis jedis) {
        String key = "`Key";

        // Keep #0 element
        setupJedisForLTrim(jedis, key);
        String result = jedis.ltrim(key, 0, 0);
        assertEquals("OK", result);
        assertEquals("e0", ltrimResult(jedis, key));

        // Keep #1 element
        setupJedisForLTrim(jedis, key);
        result = jedis.ltrim(key, 1, 1);
        assertEquals("OK", result);
        assertEquals("e1", ltrimResult(jedis, key));

        // Keep #2 element
        setupJedisForLTrim(jedis, key);
        result = jedis.ltrim(key, 2, 2);
        assertEquals("OK", result);
        assertEquals("e2", ltrimResult(jedis, key));

        // Keep #-3 element
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -3, -3);
        assertEquals("e0", ltrimResult(jedis, key));

        // Keep #-2 element
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -2, -2);
        assertEquals("e1", ltrimResult(jedis, key));

        // Keep #-1 element
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -1, -1);
        assertEquals("e2", ltrimResult(jedis, key));

        // Keep #0-1 elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 0, 1);
        assertEquals("e0,e1", ltrimResult(jedis, key));

        // Keep #1-2 elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 1, 2);
        assertEquals("e1,e2", ltrimResult(jedis, key));

        // Keep #0-2 elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 0, 2);
        assertEquals("e0,e1,e2", ltrimResult(jedis, key));

        // Keep #0-2 elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 0, -1);
        assertEquals("e0,e1,e2", ltrimResult(jedis, key));

        // Keep #1-2 elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 1, -1);
        assertEquals("e1,e2", ltrimResult(jedis, key));

        // Keep #2 element
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 2, -1);
        assertEquals("e2", ltrimResult(jedis, key));

        // Keep #0-1 elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 0, -2);
        assertEquals("e0,e1", ltrimResult(jedis, key));

        // Keep #0 element
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 1, -2);
        assertEquals("e1", ltrimResult(jedis, key));

        // Remove all elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, 2, -2);
        assertEquals("", ltrimResult(jedis, key));

        // Keep #0 element
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -4, 0);
        assertEquals("e0", ltrimResult(jedis, key));

        // Keep all elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -4, -1);
        assertEquals("e0,e1,e2", ltrimResult(jedis, key));

        // Keep all elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -4, 5);
        assertEquals("e0,e1,e2", ltrimResult(jedis, key));

        // Keep all elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -5, 5);
        assertEquals("e0,e1,e2", ltrimResult(jedis, key));

        // Remove all elements
        setupJedisForLTrim(jedis, key);
        jedis.ltrim(key, -5, -5);
        assertEquals("", ltrimResult(jedis, key));
    }

    private void setupJedisForLTrim(Jedis jedis, String key) {
        jedis.del(key);
        jedis.rpush(key, "e0");
        jedis.rpush(key, "e1");
        jedis.rpush(key, "e2");
    }

    private String ltrimResult(Jedis jedis, String key) {
        return String.join(",", jedis.lrange(key, 0, -1));
    }

    @TestTemplate
    public void testGetOperation(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertThrows(JedisDataException.class, () -> jedis.get("Another key"));
    }
}
