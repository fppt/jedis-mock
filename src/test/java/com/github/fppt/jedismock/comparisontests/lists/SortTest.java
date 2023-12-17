package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.SortingParams;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testcontainers.shaded.com.google.common.collect.Lists.reverse;
import static redis.clients.jedis.args.SortingOrder.ASC;
import static redis.clients.jedis.args.SortingOrder.DESC;

@ExtendWith(ComparisonBase.class)
public class SortTest {

    private static final String key = "sort_key";
    private static final String numerical_sort_key = "numerical_sort_key";
    private static final String store_sort_key = "store_sort_key";

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();

        jedis.rpush(key, "a", "b", "c", "1", "2", "3", "c", "c");
        jedis.rpush(numerical_sort_key, "5", "4", "3", "2", "1");
    }

    @TestTemplate
    public void whenUsingSort_EnsureSortsNumerical(Jedis jedis) {
        assertThat(jedis.sort(numerical_sort_key)).containsExactly("1", "2", "3", "4", "5");
        assertThat(jedis.sort(numerical_sort_key, new SortingParams().sortingOrder(DESC))).containsExactly("5", "4", "3", "2", "1");
        assertThat(jedis.sort(numerical_sort_key, new SortingParams().sortingOrder(ASC))).containsExactly("1", "2", "3", "4", "5");
    }

    @TestTemplate
    public void whenUsingSort_EnsureSortsAlphabetically(Jedis jedis) {
        List<String> result = Arrays.asList("1", "2", "3", "a", "b", "c", "c", "c");
        assertThat(jedis.sort(key, new SortingParams().sortingOrder(ASC).alpha())).isEqualTo(result);
        assertThat(jedis.sort(key, new SortingParams().sortingOrder(DESC).alpha())).isEqualTo(reverse(result));
    }

    @TestTemplate
    public void whenUsingSort_EnsureStores(Jedis jedis) {
        assertThat(jedis.sort(numerical_sort_key, new SortingParams().sortingOrder(DESC), store_sort_key)).isEqualTo(5);
        assertThat(jedis.lrange(numerical_sort_key, 0, 5)).containsExactly("5", "4", "3", "2", "1");
    }

    @TestTemplate
    public void whenUsingSort_EnsureThrowsOnInvalidType(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sort(key))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR One or more scores can't be converted into double");
    }

    @TestTemplate
    public void whenUsingSort_EnsureHandlesLimit(Jedis jedis) {
        List<String> sortedResult = Arrays.asList("1", "2", "3", "a", "b", "c", "c", "c");
        assertThat(jedis.sort(key, new SortingParams().alpha().limit(1, 3))).isEqualTo(sortedResult.subList(1, 4));
        assertThat(jedis.sort(key, new SortingParams().alpha().limit(1, 100))).isEqualTo(sortedResult.subList(1, 8));
        assertThat(jedis.sort(key, new SortingParams().alpha().limit(0, 4))).isEqualTo(sortedResult.subList(0, 4));
        assertThat(jedis.sort(key, new SortingParams().alpha().limit(-100, 4))).isEqualTo(sortedResult.subList(0, 4));
    }

    @TestTemplate
    @Timeout(value = 3)
    public void whenUsingSort_EnsureWakesOnStore(Jedis jedis, HostAndPort hostAndPort) throws InterruptedException, ExecutionException {
        Jedis blockingClient = new Jedis(hostAndPort);
        ExecutorService e = Executors.newSingleThreadExecutor();

        Future<?> future = e.submit(() -> {
            List<String> result = blockingClient.blpop(0, store_sort_key);

            assertThat(result).containsExactly(store_sort_key, "1");
        });

        Thread.sleep(100);

        jedis.sort(numerical_sort_key, new SortingParams(), store_sort_key);
        future.get();
        e.shutdownNow();
        blockingClient.close();
    }
}