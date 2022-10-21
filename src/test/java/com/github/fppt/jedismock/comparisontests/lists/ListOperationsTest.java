package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

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
        assertEquals(jedis.lpop(key), "1");
    }

    @TestTemplate
    public void whenUsingLpopCount_EnsureAllElementsPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3", "4");
        List<String> results1 = jedis.lpop(key, 3);
        assertEquals(3, results1.size());
        List<String> results2 = jedis.lpop(key, 5);
        assertEquals(1, results2.size());
        List<String> results3 = jedis.lpop(key, 5);
        assertNull(results3);
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
    public void testGetOperation(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertThrows(JedisDataException.class, () -> jedis.get("Another key"));
    }
}
