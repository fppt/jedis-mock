package com.github.fppt.jedismock.comparisontests.hyperloglog;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class HLLOperationsTest {
    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void testPfadd(Jedis jedis) {
        jedis.pfadd("my_hll", "a", "b", "c", "e", "d", "f");
        assertThat(jedis.pfcount("my_hll")).isEqualTo(6);
        assertThat(jedis.pfadd("my_hll", "a")).isEqualTo(0);
    }

    @TestTemplate
    public void testPfcount(Jedis jedis) {
        String[] arr = {"a", "b", "c", "e", "d", "f"};
        for (int i = 0; i < 6; ++i) {
            jedis.pfadd("my_hll", arr[i]);
            assertThat(jedis.pfcount("my_hll")).isEqualTo(i + 1);
        }
    }

    @TestTemplate
    public void testPfmerge(Jedis jedis) {
        jedis.pfadd("hll1", "a", "b", "c", "d");
        jedis.pfadd("hll2", "d", "e", "f");
        jedis.pfmerge("hll3", "hll1", "hll2");
        assertThat(jedis.pfcount("hll3")).isEqualTo(6);
    }

    @TestTemplate
    public void testGetOperation(Jedis jedis) {
        jedis.pfadd("foo1", "bar");
        byte[] buf = jedis.get("foo1".getBytes());
        jedis.set("foo2".getBytes(), buf);
        assertThat(jedis.pfcount("foo1")).isEqualTo(1);
        assertThat(jedis.pfcount("foo2")).isEqualTo(1);
    }

    @TestTemplate
    public void testGetOperationRepeatable(Jedis jedis) {
        jedis.pfadd("foo1", "bar");
        byte[] buf = jedis.get("foo1".getBytes());
        jedis.set("foo2".getBytes(), buf);
        byte[] buf2 = jedis.get("foo2".getBytes());
        assertThat(buf2).containsExactlyInAnyOrder(buf);
    }

    @TestTemplate
    public void testFailingGetOperation(Jedis jedis) {
        jedis.set("not_a_hll", "bar");
        assertThatThrownBy(() -> jedis.pfadd("not_a_hll", "value"))
                .isInstanceOf(JedisDataException.class)
                .hasMessageStartingWith("WRONGTYPE");
    }

    @TestTemplate
    public void testGetSet(Jedis jedis) {
        jedis.pfadd("my_hll", "a", "b", "c", "e", "d", "f");
        jedis.set("another".getBytes(), jedis.get("my_hll".getBytes()));
        assertThat(jedis.pfcount("another")).isEqualTo(6);
    }
}
