package com.github.fppt.jedismock.comparisontests.transactions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.testcontainers.shaded.org.awaitility.Awaitility;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import java.time.Duration;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Port of the {@code unit/multi.tcl} test "WATCH will consider touched expired
 * keys": a key that expires <em>after</em> it is watched must be treated as
 * modified, so a following {@code MULTI}/.../{@code EXEC} aborts and returns nil.
 *
 * <p>The subtlety the mock got wrong: the tcl test forces a {@code DBSIZE} sweep
 * (via {@code wait_for_dbsize 0}) before {@code EXEC}. That sweep purged the
 * expired key, but silently — without flagging the watcher — so the later
 * {@code EXEC} could no longer notice the key had expired and ran the
 * transaction instead of aborting it.
 */
@ExtendWith(ComparisonBase.class)
public class WatchExpiredKeyComparisonTest {

    @TestTemplate
    public void watchConsidersTouchedExpiredKeys(Jedis jedis) {
        jedis.flushAll();
        jedis.del("x");
        jedis.set("x", "foo");
        jedis.pexpire("x", 50);
        jedis.watch("x");

        // Wait for the key to expire AND for a DBSIZE sweep to observe it gone —
        // mirroring the tcl `wait_for_dbsize 0`, which is what purges the key.
        Awaitility.await().atMost(Duration.ofSeconds(2))
                .until(() -> jedis.dbSize() == 0);

        // The watched key expired, so the transaction must abort: EXEC -> nil.
        Transaction transaction = jedis.multi();
        transaction.get("x");
        assertThat(transaction.exec())
                .as("EXEC must abort (nil) because the watched key expired")
                .isNull();
    }
}
