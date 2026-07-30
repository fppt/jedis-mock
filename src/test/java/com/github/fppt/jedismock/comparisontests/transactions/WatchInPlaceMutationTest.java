package com.github.fppt.jedismock.comparisontests.transactions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.args.ListPosition;

import java.util.List;
import java.util.function.Consumer;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Commands that mutate a list or stream in place — rather than replacing the
 * value — must still count as touching the key, so a transaction watching it
 * is aborted. A command that turns out to change nothing must not abort one.
 */
@ExtendWith(ComparisonBase.class)
public class WatchInPlaceMutationTest {

    private static final String KEY = "watched_list";

    private Jedis anotherJedis;

    @BeforeEach
    public void setup(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        anotherJedis = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
    }

    @AfterEach
    public void tearDown() {
        anotherJedis.close();
    }

    /** @return the EXEC result: null when the transaction was aborted. */
    private List<Object> execAfterInterference(Jedis jedis, Consumer<Jedis> interference) {
        jedis.watch(KEY);
        interference.accept(anotherJedis);
        Transaction transaction = jedis.multi();
        transaction.set("sentinel", "written");
        return transaction.exec();
    }

    @TestTemplate
    public void inPlaceListMutationsAbortAWatchingTransaction(Jedis jedis) {
        jedis.rpush(KEY, "a", "b", "c");
        assertThat(execAfterInterference(jedis, other -> other.lset(KEY, 0, "z")))
                .as("LSET must abort").isNull();

        jedis.del(KEY);
        jedis.rpush(KEY, "a", "b", "c");
        assertThat(execAfterInterference(jedis, other -> other.linsert(KEY, ListPosition.BEFORE, "a", "q")))
                .as("LINSERT must abort").isNull();

        jedis.del(KEY);
        jedis.rpush(KEY, "a", "b", "c");
        assertThat(execAfterInterference(jedis, other -> other.lrem(KEY, 0, "a")))
                .as("LREM must abort").isNull();

        jedis.del(KEY);
        jedis.rpush(KEY, "a", "b", "c");
        assertThat(execAfterInterference(jedis, other -> other.ltrim(KEY, 0, 1)))
                .as("LTRIM must abort").isNull();
    }

    @TestTemplate
    public void removingAnAbsentElementDoesNotAbortAWatchingTransaction(Jedis jedis) {
        jedis.rpush(KEY, "a", "b", "c");
        //LREM only counts as touching the key when it removed something
        assertThat(execAfterInterference(jedis, other -> other.lrem(KEY, 0, "absent")))
                .as("no-op LREM must not abort").isNotNull();
    }

    @TestTemplate
    public void trimmingNothingStillAbortsAWatchingTransaction(Jedis jedis) {
        jedis.rpush(KEY, "a", "b", "c");
        //Unlike LREM, LTRIM signals the key as modified unconditionally, even
        //when the range covers the whole list and nothing is removed
        assertThat(execAfterInterference(jedis, other -> other.ltrim(KEY, 0, -1)))
                .as("no-op LTRIM aborts anyway").isNull();
    }
}
