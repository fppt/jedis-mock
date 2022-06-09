package com.github.fppt.jedismock.comparisontests.hyperloglog;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith(ComparisonBase.class)
public class HLLOperationsTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testPfadd(Jedis jedis) {
        jedis.pfadd("my_hll", "a", "b", "c", "e", "d", "f");
        assertEquals(jedis.pfcount("my_hll"), 6);
        assertEquals(jedis.pfadd("my_hll", "a"), 0);
    }

    @TestTemplate
    public void testPfcount(Jedis jedis) {
        String[] arr = {"a", "b", "c", "e", "d", "f"};
        for (int i = 0; i < 6; ++i) {
            jedis.pfadd("my_hll", arr[i]);
            assertEquals(jedis.pfcount("my_hll"), i + 1);
        }
    }

    @TestTemplate
    public void testPfmerge(Jedis jedis) {
        jedis.pfadd("hll1", "a", "b", "c", "d");
        jedis.pfadd("hll2", "d", "e", "f");
        jedis.pfmerge("hll3", "hll1", "hll2");
        assertEquals(6, jedis.pfcount("hll3"));
    }

    @TestTemplate
    public void testGetOperation(Jedis jedis) {
        jedis.pfadd("foo1", "bar");
        byte[] buf = jedis.get("foo1".getBytes());
        jedis.set("foo2".getBytes(), buf);
        assertEquals(1, jedis.pfcount("foo1"));
        assertEquals(1, jedis.pfcount("foo2"));
    }

    @TestTemplate
    public void testGetOperationRepeatable(Jedis jedis) {
        jedis.pfadd("foo1", "bar");
        byte[] buf = jedis.get("foo1".getBytes());
        jedis.set("foo2".getBytes(), buf);
        byte[] buf2 = jedis.get("foo2".getBytes());
        assertArrayEquals(buf, buf2);
    }

    @TestTemplate
    public void testFailingGetOperation(Jedis jedis) {
        jedis.set("not_a_hll", "bar");
        assertTrue(
                assertThrows(JedisDataException.class, () ->
                        jedis.pfadd("not_a_hll", "value")).getMessage().startsWith("WRONGTYPE"));
    }

    @TestTemplate
    public void testGetSet(Jedis jedis) {
        jedis.pfadd("my_hll", "a", "b", "c", "e", "d", "f");
        jedis.set("another".getBytes(), jedis.get("my_hll".getBytes()));
        assertEquals(jedis.pfcount("another"), 6);
    }
}
