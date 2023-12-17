package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

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
        assertThat(jedis.rpop(key)).isEqualTo("3");
    }

    @TestTemplate
    public void whenUsingLpop_EnsureTheFirstElementPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertThat(jedis.lpop(key)).isEqualTo("1");
    }

    @TestTemplate
    public void whenUsingLpopCount_EnsureAllElementsPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3", "4");
        assertThat(jedis.lpop(key, 3)).containsExactly("1", "2", "3");
        assertThat(jedis.lpop(key, 5)).containsExactly("4");
        assertThat(jedis.lpop(key, 5)).isNull();
    }

    @TestTemplate
    public void whenUsingRpop_EnsureTheFirstElementPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertThat(jedis.rpop(key)).isEqualTo("3");
    }

    @TestTemplate
    public void whenUsingRpopCount_EnsureAllElementsPushedIsReturned(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3", "4");
        assertThat(jedis.rpop(key, 3)).containsExactly("4", "3", "2");
        assertThat(jedis.rpop(key, 5)).containsExactly("1");
        assertThat(jedis.rpop(key, 5)).isNull();
    }

    @TestTemplate
    public void whenUsingRpoplpush_CorrectResultsAreReturned(Jedis jedis) {
        String list1key = "list 1";
        String list2key = "list 2";

        String nullResult = jedis.rpoplpush(list1key, list2key);
        assertThat(nullResult).isNull();

        jedis.rpush(list1key, "1", "2", "3");
        jedis.rpush(list2key, "a", "b", "c");

        //Check the lists are in order
        List<String> results1 = jedis.lrange(list1key, 0, -1);
        List<String> results2 = jedis.lrange(list2key, 0, -1);

        assertThat(results1).contains("1", "2", "3");

        assertThat(results2).contains("a", "b", "c");

        //Check that the one list has been pushed into the other
        String result = jedis.rpoplpush(list1key, list2key);
        assertThat(result).isEqualTo("3");

        results1 = jedis.lrange(list1key, 0, -1);
        results2 = jedis.lrange(list2key, 0, -1);

        assertThat(results1).contains("1", "2");
        assertThat(results1.contains("3")).isFalse();

        assertThat(results2).contains("3", "a", "b", "c");
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
        assertThat(list).contains(hello, hello, foo, hello);

        long numRemoved = jedis.lrem(key, -2, hello);
        assertThat(numRemoved).isEqualTo(2L);

        //Check order again
        list = jedis.lrange(key, 0, -1);
        assertThat(list).contains(hello, foo);
    }

    @TestTemplate
    public void testGetOperation(Jedis jedis) {
        String key = "Another key";
        jedis.rpush(key, "1", "2", "3");
        assertThatThrownBy(() -> jedis.get("Another key"))
                .isInstanceOf(JedisDataException.class);
    }
}
