package com.github.fppt.jedismock.comparisontests.transactions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class TransactionOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenTransactionWithMultiplePushesIsExecuted_EnsureResultsAreSaved(Jedis jedis) {
        String key = "my-list";
        assertEquals(new Long(0), jedis.llen(key));

        Transaction transaction = jedis.multi();
        transaction.lpush(key, "1");
        transaction.lpush(key, "2");
        transaction.lpush(key, "3");
        transaction.exec();

        assertEquals(new Long(3), jedis.llen(key));
    }

    @TestTemplate
    public void whenDiscardIsExecuted_EnsureResultsAreDiscarded(Jedis jedis) {
        String key = "my-list";
        assertEquals(new Long(0), jedis.llen(key));

        Transaction transaction = jedis.multi();
        transaction.lpush(key, "1");
        transaction.lpush(key, "2");
        transaction.discard();
        jedis.lpush(key, "3");

        assertEquals(new Long(1), jedis.llen(key));
    }

    @TestTemplate
    public void whenUsingTransactionAndTryingToAccessJedis_Throw(Jedis jedis) {
        //Do Something random with Jedis
        assertNull(jedis.get("oobity-oobity-boo"));

        //Start transaction
        jedis.multi();
        assertEquals("Cannot use Jedis when in Multi. Please use Transaction or reset jedis state.",
                assertThrows(JedisDataException.class, () ->
                        jedis.get("oobity-oobity-boo")).getMessage());
    }
}
