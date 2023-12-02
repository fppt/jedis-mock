package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.SortedSetOption;
import redis.clients.jedis.resps.Tuple;
import redis.clients.jedis.util.KeyValue;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

@ExtendWith(ComparisonBase.class)
public class TestBZMPop {

    private ExecutorService blockingThread;
    private Jedis blockedClient;
    private Jedis blockedClient2;
    private static final String ZSET_KEY = "myzset";

    @BeforeEach
    public void setUp(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockedClient2 = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockingThread = Executors.newFixedThreadPool(4);
    }

    @TestTemplate
    public void testBZMPopKeyNotExist(Jedis jedis) {
            assertNull(jedis.bzmpop(1, SortedSetOption.MIN, "aaa"));
    }

    @TestTemplate
    public void testBZMPopFromOneKey(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 1, "one");
        jedis.zadd(ZSET_KEY, 2, "two");
        jedis.zadd(ZSET_KEY, 3, "three");
        KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY, Collections.singletonList(new Tuple("one", 1.0)));
        assertEquals(expected, jedis.bzmpop(1, SortedSetOption.MIN, ZSET_KEY));

        List<Tuple> tupleList = Arrays.asList(new Tuple("two", 2.), new Tuple("three", 3.));
        assertEquals(tupleList, jedis.zrangeWithScores(ZSET_KEY, 0, -1));
    }

    @TestTemplate
    public void testBZMPopWithSingleExistZSet(Jedis jedis) {
        jedis.zadd(ZSET_KEY, 0, "a");
        jedis.zadd(ZSET_KEY, 1, "b");
        jedis.zadd(ZSET_KEY, 2, "c");
        jedis.zadd(ZSET_KEY, 3, "d");
        KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY, Collections.singletonList(new Tuple("a", 0.0)));
        assertEquals(expected, jedis.bzmpop(5, SortedSetOption.MIN, ZSET_KEY));
    }

    @TestTemplate
    public void testBZMPopNegativeCount(Jedis jedis) {
        assertThrows(RuntimeException.class, () ->
                jedis.bzmpop(1, SortedSetOption.MIN, -1, "aaa")
        );
    }

    @TestTemplate
    public void testBZMPopEnsureItTimeout(Jedis jedis) throws ExecutionException, InterruptedException {
        String key1 = "qwe";
        String key2 = "asd";
        String key3 = "zxc";

        Future<?> future = blockingThread.submit(() -> {
            KeyValue<String, List<Tuple>> result = blockedClient.bzmpop(0, SortedSetOption.MIN, 10, key1);
            KeyValue<String, List<Tuple>> expected = new KeyValue<>(key1, Collections.singletonList(new Tuple("zset", 1.0)));
            assertEquals(expected, result);
        });

        Future<?> future2 = blockingThread.submit(() -> {
            KeyValue<String, List<Tuple>> result = blockedClient2.bzmpop(0, SortedSetOption.MIN, 10, key2, key3);
            KeyValue<String, List<Tuple>> expected = new KeyValue<>(key3, Collections.singletonList(new Tuple("zset3", 2.0)));
            assertEquals(expected, result);
        });

        jedis.zadd("0",100,"timeout_value");
        jedis.zadd("1",200,"numkeys_value");
        jedis.zadd("min",300,"min_token");
        jedis.zadd("max",400,"max_token");
        jedis.zadd("count",500,"count_token");
        jedis.zadd("10",600,"count_value");

        jedis.zadd(key1,1,"zset");
        jedis.zadd(key3,2,"zset3");
        future.get();
        future2.get();
    }

    @TestTemplate
    public void testBZMPopNotAwake(Jedis jedis) throws ExecutionException, InterruptedException {
        Future<?> future = blockingThread.submit(() -> {
            KeyValue<String, List<Tuple>> result = blockedClient.bzmpop(0, SortedSetOption.MIN, 10, ZSET_KEY);
            KeyValue<String, List<Tuple>> expected = new KeyValue<>(ZSET_KEY, Collections.singletonList(new Tuple("bar", 1.0)));
            assertEquals(expected, result);
        });

        Transaction multi = jedis.multi();
        multi.zadd(ZSET_KEY, 0, "foo");
        multi.del(ZSET_KEY);
        multi.exec();
        jedis.del(ZSET_KEY);
        jedis.zadd(ZSET_KEY,1,"bar");
        future.get();
    }

    @TestTemplate
    public void testBZMPopMultipleBlockingClients(Jedis jedis) throws ExecutionException, InterruptedException {
        String key1 = "qwe";
        String key2 = "asd";

        Future<?> future = blockingThread.submit(() -> {
            KeyValue<String, List<Tuple>> result = blockedClient.bzmpop(0, SortedSetOption.MIN, 1, key1, key2);
            KeyValue<String, List<Tuple>> expected = new KeyValue<>(key1, Collections.singletonList(new Tuple("a", 1.0)));
            assertEquals(expected, result);

            KeyValue<String, List<Tuple>> result2 = blockedClient2.bzmpop(0, SortedSetOption.MAX, 10, key1, key2);
            KeyValue<String, List<Tuple>> expected2 = new KeyValue<>(key1, Arrays.asList(
                    new Tuple("e", 5.0),
                    new Tuple("d", 4.0),
                    new Tuple("c", 3.0),
                    new Tuple("b", 2.0)));
            assertEquals(expected2, result2);

        });

        Transaction multi = jedis.multi();
        multi.zadd(key1, 1, "a");
        multi.zadd(key1, 2, "b");
        multi.zadd(key1, 3, "c");
        multi.zadd(key1, 4, "d");
        multi.zadd(key1, 5, "e");
        multi.zadd(key2, 1, "a");
        multi.zadd(key2, 2, "b");
        multi.zadd(key2, 3, "c");
        multi.zadd(key2, 4, "d");
        multi.zadd(key2, 5, "e");
        multi.exec();

        Thread.sleep(200);
        Map<String, Double> members = new HashMap<>();
        members.put("a", 1.);
        members.put("b", 2.);
        members.put("c", 3.);
        jedis.zadd(key2, members);
        future.get();
    }
}
