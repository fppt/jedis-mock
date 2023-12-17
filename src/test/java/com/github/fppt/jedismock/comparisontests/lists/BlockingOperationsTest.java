package com.github.fppt.jedismock.comparisontests.lists;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import com.github.fppt.jedismock.comparisontests.TestErrorMessages;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;

import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class BlockingOperationsTest {

    private ExecutorService blockingThread;
    private Jedis blockedClient;

    @BeforeEach
    public void setUp(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockingThread = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown() {
        blockedClient.close();
        blockingThread.shutdownNow();
    }

    @TestTemplate
    public void whenUsingBrpoplpush_EnsureItBlocksAndCorrectResultsAreReturned(Jedis jedis) throws ExecutionException, InterruptedException {
        String list1key = "source list";
        String list2key = "target list";

        jedis.rpush(list2key, "a", "b", "c");

        //Block on performing the BRPOPLPUSH
        Future<?> future = blockingThread.submit(() -> {
            String result = blockedClient.brpoplpush(list1key, list2key, 500);
            assertThat(result).isEqualTo("3");
        });

        //Check the list is not modified
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertThat(results).hasSize(3);

        //Push some stuff into the blocked list
        jedis.rpush(list1key, "1", "2", "3");

        future.get();

        //Check the list is modified
        results = jedis.lrange(list2key, 0, -1);
        assertThat(results).hasSize(4);
    }

    @TestTemplate
    public void whenUsingBrpoplpushAndReachingTimeout_Return(Jedis jedis) {
        String list1key = "another source list";
        String list2key = "another target list";

        String result = jedis.brpoplpush(list1key, list2key, 1);

        assertThat(result).isNull();
    }

    @TestTemplate
    public void whenUsingBrpoplpush_EnsureClientCanStillGetOtherResponsesInTimelyManner(Jedis jedis) {
        String list1key = "another another source list";
        String list2key = "another another target list";

        blockingThread.submit(() -> {
            String result = blockedClient.brpoplpush(list1key, list2key, 500);
            assertThat(result).isEqualTo("3");
        });

        //Issue random commands to make sure mock is still responsive
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.set("k3", "v3");
        jedis.set("k4", "v4");
        jedis.set("k5", "v5");

        //Check random commands were processed
        assertThat(jedis.get("k1")).isEqualTo("v1");
        assertThat(jedis.get("k2")).isEqualTo("v2");
        assertThat(jedis.get("k3")).isEqualTo("v3");
        assertThat(jedis.get("k4")).isEqualTo("v4");
        assertThat(jedis.get("k5")).isEqualTo("v5");
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItBlocksAndCorrectResultsAreReturned(Jedis jedis) throws ExecutionException, InterruptedException {
        String key = "list1_kfubdjkfnv";
        jedis.rpush(key, "d", "e", "f");
        //Block on performing the BLPOP
        Future<?> future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(10, key);
            assertThat(result).containsExactly(key, "d");
        });
        future.get();
        //Check the list is modified
        List<String> results = jedis.lrange(key, 0, -1);
        assertThat(results).hasSize(2);
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItBlocksAndCorrectResultsAreReturnedOnMultipleList(Jedis jedis) throws ExecutionException, InterruptedException {
        String list1key = "list1_dkjfnvdk";
        String list2key = "list2_kjvnddkf";
        String list3key = "list3_oerurthv";


        //Block on performing the BLPOP
        Future<?> future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(10, list1key, list2key, list3key);
            assertThat(result).containsExactly(list2key, "a");
        });
        Thread.sleep(1000);
        jedis.rpush(list2key, "a", "b", "c");
        jedis.rpush(list3key, "d", "e", "f");
        future.get();

        //Check the list is modified
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertThat(results).hasSize(2);
        results = jedis.lrange(list3key, 0, -1);
        assertThat(results).hasSize(3);
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItTimeout(Jedis jedis) throws ExecutionException, InterruptedException, TimeoutException {
        String list1key = "list1_kdjfnvdsu";
        String list2key = "list2_mbhkdushy";
        String list3key = "list3_qzkmpthju";

        // init redisbase
        jedis.lrange(list2key, 0, -1);

        //Block on performing the BLPOP
        Future<?> future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(1, list1key, list2key, list3key);
            assertThat(result).isNull();
        });
        //Check the list is not modified
        jedis.getClient().setSoTimeout(2000);
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertThat(results).isEmpty();
        future.get(4, TimeUnit.SECONDS);
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureNotWokenByTransaction(Jedis jedis) throws ExecutionException, InterruptedException, TimeoutException {
        String listKey = "list_key";

        Future<?> future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(0, listKey);
            assertThat(result).isNotNull();
            assertThat(result).containsExactly(listKey, "b");
        });

        Transaction t = jedis.multi();
        t.lpush(listKey, "0");
        t.del(listKey);
        t.exec();
        jedis.del(listKey);
        jedis.lpush(listKey, "b");
        future.get(5, TimeUnit.SECONDS);
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureDoesntBlockInsideTransaction(Jedis jedis) {
        String key = "blpop_transaction_key";

        Assertions.assertTimeoutPreemptively(Duration.ofSeconds(5), () -> {
            jedis.del(key);
            jedis.lpush(key, "foo");
            jedis.lpush(key, "bar");

            Transaction t = jedis.multi();

            t.blpop(0, key);
            t.blpop(0, key);
            t.blpop(0, key);

            List<Object> result = t.exec();

            assertThat(result).containsExactlyElementsOf(Arrays.asList(
                    Arrays.asList(key, "bar"),
                    Arrays.asList(key, "foo"),
                    null
            ));

        }, TestErrorMessages.DEADLOCK_ERROR_MESSAGE);
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureThrowsErrorOnNegativeTimeout(Jedis jedis) {
        String key = "blpop_negative_timeout_key";
        assertThatThrownBy(() -> jedis.blpop(-5, key))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR timeout is negative");
        assertThatThrownBy(() -> jedis.blpop(-0.1, key))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR timeout is negative");
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureStillWaitsIfKeyIsNotList(Jedis jedis) throws ExecutionException, InterruptedException, TimeoutException {
        String key = "blpop_not_a_list_key";


        Future<?> future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(0, key);
            assertThat(result).containsExactly(key, "foo");
        });

        Thread.sleep(300); // wait for blpop to execute

        Transaction t = jedis.multi();

        t.rpush(key, "bar");
        t.del(key);
        t.set(key, "bar2");
        t.exec();

        jedis.del(key);
        jedis.lpush(key, "foo");

        future.get(5, TimeUnit.SECONDS);
    }

    @TestTemplate
    public void whenUsingBRPopLPush_ensureBlocksIndefinitely(Jedis jedis) throws InterruptedException, ExecutionException {
        String fromKey = "brpoplpush_from";
        String toKey = "brpoplpush_to";


        Future<?> future = blockingThread.submit(() -> {
            String value = blockedClient.brpoplpush(fromKey, toKey, 0);
            assertThat(value).isEqualTo("bar");
        });

        // wait to be sure we wait more than 0 seconds
        Thread.sleep(1000);
        jedis.rpush(fromKey, "bar");

        future.get();
    }
}