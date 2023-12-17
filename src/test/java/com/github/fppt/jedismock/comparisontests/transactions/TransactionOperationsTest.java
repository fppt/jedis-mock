package com.github.fppt.jedismock.comparisontests.transactions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Transaction;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TransactionOperationsTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void whenTransactionWithMultiplePushesIsExecuted_EnsureResultsAreSaved(Jedis jedis) {
        String key = "my-list";
        assertThat(jedis.llen(key)).isEqualTo(0);

        Transaction transaction = jedis.multi();
        transaction.lpush(key, "1");
        transaction.lpush(key, "2");
        transaction.lpush(key, "3");
        transaction.exec();

        assertThat(jedis.llen(key)).isEqualTo(3);
    }

    @TestTemplate
    public void whenDiscardIsExecuted_EnsureResultsAreDiscarded(Jedis jedis) {
        String key = "my-list";
        assertThat(jedis.llen(key)).isEqualTo(0L);

        Transaction transaction = jedis.multi();
        transaction.lpush(key, "1");
        transaction.lpush(key, "2");
        transaction.discard();
        jedis.lpush(key, "3");

        assertThat(jedis.llen(key)).isEqualTo(1);
    }

    @TestTemplate
    public void whenUsingTransactionAndTryingToAccessJedis_Throw(Jedis jedis) {
        //Do Something random with Jedis
        assertThat(jedis.get("oobity-oobity-boo")).isNull();

        //Start transaction
        jedis.multi();
        assertThatThrownBy(() -> jedis.get("oobity-oobity-boo"))
                .isInstanceOf(IllegalStateException.class)
                .hasMessage("Cannot use Jedis when in Multi. Please use Transaction or reset jedis state.");
    }
}
