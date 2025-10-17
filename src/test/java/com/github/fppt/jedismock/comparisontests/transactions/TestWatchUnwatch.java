package com.github.fppt.jedismock.comparisontests.transactions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.params.SetParams;

import java.util.List;
import java.util.concurrent.ExecutionException;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class TestWatchUnwatch {
    private static final String FIRST_KEY = "first_key";
    private static final String SECOND_KEY = "second_key";
    private static final String ANOTHER_KEY = "another_key";
    private static final String FIRST_VALUE = "first_value";
    private static final String SECOND_VALUE = "second_value";
    private static final String ANOTHER_VALUE = "another_value";

    private Jedis anotherJedis;

    @BeforeEach
    public void setup(Jedis jedis, HostAndPort hostAndPort) {
        jedis.del(FIRST_KEY);
        jedis.del(ANOTHER_KEY);

        anotherJedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
    }

    @TestTemplate
    public void testWatchWithKeySetting(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        Transaction transaction = jedis.multi();
        runAsync(() -> anotherJedis.set(FIRST_KEY, ANOTHER_VALUE)).get();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithChangingToTheSameValue(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        Transaction transaction = jedis.multi();
        runAsync(() -> anotherJedis.set(FIRST_KEY, FIRST_VALUE)).get();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithHSet(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.hset(FIRST_KEY, SECOND_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        runAsync(() -> anotherJedis.hset(FIRST_KEY, ANOTHER_KEY, ANOTHER_VALUE)).get();
        Transaction transaction = jedis.multi();
        transaction.hset(FIRST_KEY, SECOND_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithSet(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.sadd(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        runAsync(() -> anotherJedis.sadd(FIRST_KEY, ANOTHER_VALUE)).get();
        Transaction transaction = jedis.multi();
        transaction.set(SECOND_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithNoKeyAffection(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        Transaction transaction = jedis.multi();
        runAsync(() -> anotherJedis.set(ANOTHER_KEY, ANOTHER_VALUE)).get();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).containsExactly("OK");
        assertThat(jedis.get(FIRST_KEY)).isEqualTo(SECOND_VALUE);
    }

    @TestTemplate
    public void testWatchWithKeyExpiring(Jedis jedis) throws InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE, new SetParams().px(100));
        jedis.watch(FIRST_KEY);
        Transaction transaction = jedis.multi();
        Thread.sleep(200);
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithKeyDeleting(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        runAsync(() -> anotherJedis.del(FIRST_KEY)).get();
        Transaction transaction = jedis.multi();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithTTLChanging(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        runAsync(() -> anotherJedis.pexpire(FIRST_KEY, 1000)).get();
        Transaction transaction = jedis.multi();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithUnwatch(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY);
        runAsync(() -> anotherJedis.del(FIRST_KEY)).get();
        jedis.unwatch();
        Transaction transaction = jedis.multi();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).containsExactly("OK");
        assertThat(jedis.get(FIRST_KEY)).isEqualTo(SECOND_VALUE);
    }

    @TestTemplate
    public void testWatchWithMultipleKeys(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.set(FIRST_KEY, FIRST_VALUE);
        jedis.watch(FIRST_KEY, SECOND_KEY);
        runAsync(() -> anotherJedis.set(SECOND_KEY, ANOTHER_VALUE)).get();
        Transaction transaction = jedis.multi();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).isNull();
    }

    @TestTemplate
    public void testWatchWithNoKeyInStorage(Jedis jedis) {
        jedis.watch(FIRST_KEY, SECOND_KEY);
        Transaction transaction = jedis.multi();
        transaction.set(FIRST_KEY, SECOND_VALUE);
        List<Object> result = transaction.exec();
        assertThat(result).containsExactly("OK");
    }


    @TestTemplate
    public void afterFailedExecKeyIsNoLongerWatched(Jedis jedis){
        jedis.set("x", "a");
        jedis.watch("x");
        jedis.set("x", "b");
        Transaction transaction = jedis.multi();
        transaction.set("foo", "bar");
        assertThat(transaction.exec()).isNull();
        transaction = jedis.multi();
        transaction.set("foo", "bar");
        assertThat(transaction.exec()).hasSize(1);
    }

    @TestTemplate
    void flushTouchesWatchedKeys(Jedis jedis){
        jedis.set("x", "a");
        jedis.watch("x");
        jedis.flushAll();
        Transaction transaction = jedis.multi();
        transaction.set("foo", "bar");
        assertThat(transaction.exec()).isNull();
    }
}
