package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@ExtendWith(ComparisonBase.class)
public class AdvanceListOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenUsingBrpoplpush_EnsureItBlocksAndCorrectResultsAreReturned(Jedis jedis, HostAndPort hostAndPort) throws ExecutionException, InterruptedException {
        String list1key = "source list";
        String list2key = "target list";

        jedis.rpush(list2key, "a", "b", "c");

        //Block on performing the BRPOPLPUSH
        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            String result = blockedClient.brpoplpush(list1key, list2key, 500);
            assertEquals("3", result);
        });

        //Check the list is not modified
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertEquals(3, results.size());

        //Push some stuff into the blocked list
        jedis.rpush(list1key, "1", "2", "3");

        future.get();

        //Check the list is modified
        results = jedis.lrange(list2key, 0, -1);
        assertEquals(4, results.size());
    }

    @TestTemplate
    public void whenUsingBrpoplpushAndReachingTimeout_Return(Jedis jedis) {
        String list1key = "another source list";
        String list2key = "another target list";

        String result = jedis.brpoplpush(list1key, list2key, 1);

        assertNull(result);
    }

    @TestTemplate
    public void whenUsingBrpoplpush_EnsureClientCanStillGetOtherResponsesInTimelyManner(Jedis jedis, HostAndPort hostAndPort) {
        String list1key = "another another source list";
        String list2key = "another another target list";

        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        blockingThread.submit(() -> {
            String result = blockedClient.brpoplpush(list1key, list2key, 500);
            assertEquals("3", result);
        });

        //Issue random commands to make sure mock is still responsive
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.set("k3", "v3");
        jedis.set("k4", "v4");
        jedis.set("k5", "v5");

        //Check random commands were processed
        assertEquals("v1", jedis.get("k1"));
        assertEquals("v2", jedis.get("k2"));
        assertEquals("v3", jedis.get("k3"));
        assertEquals("v4", jedis.get("k4"));
        assertEquals("v5", jedis.get("k5"));
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItBlocksAndCorrectResultsAreReturned(Jedis jedis, HostAndPort hostAndPort) throws ExecutionException, InterruptedException {
        String key = "list1_kfubdjkfnv";
        jedis.rpush(key, "d", "e", "f");
        //Block on performing the BLPOP
        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(10, key);
            assertEquals(2, result.size());
            assertEquals(key, result.get(0));
            assertEquals("d", result.get(1));
        });
        future.get();
        //Check the list is modified
        List<String> results = jedis.lrange(key, 0, -1);
        assertEquals(2, results.size());
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItBlocksAndCorrectResultsAreReturnedOnMultipleList(Jedis jedis, HostAndPort hostAndPort) throws ExecutionException, InterruptedException {
        String list1key = "list1_dkjfnvdk";
        String list2key = "list2_kjvnddkf";
        String list3key = "list3_oerurthv";


        //Block on performing the BLPOP
        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(10, list1key, list2key, list3key);
            assertEquals(list2key, result.get(0));
            assertEquals("a", result.get(1));
        });
        Thread.sleep(1000);
        jedis.rpush(list2key, "a", "b", "c");
        jedis.rpush(list3key, "d", "e", "f");
        future.get();

        //Check the list is modified
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertEquals(2, results.size());
        results = jedis.lrange(list3key, 0, -1);
        assertEquals(3, results.size());
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItTimeout(Jedis jedis, HostAndPort hostAndPort) throws ExecutionException, InterruptedException, TimeoutException {
        String list1key = "list1_kdjfnvdsu";
        String list2key = "list2_mbhkdushy";
        String list3key = "list3_qzkmpthju";

        // init redisbase
        jedis.lrange(list2key, 0, -1);

        //Block on performing the BLPOP
        Jedis blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(1, list1key, list2key, list3key);
            assertNull(result);
        });
        //Check the list is not modified
        jedis.getClient().setSoTimeout(2000);
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertEquals(0, results.size());
        future.get(4, TimeUnit.SECONDS);
    }
}
