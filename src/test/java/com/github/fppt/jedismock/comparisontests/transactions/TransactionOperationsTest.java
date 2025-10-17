package com.github.fppt.jedismock.comparisontests.transactions;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.ArrayList;

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
    public void queuedOperationShouldReplyQueued(Jedis jedis) {
        assertThat((byte[]) jedis.sendCommand(Protocol.Command.MULTI)).asString().isEqualTo("OK");
        assertThat((byte[]) jedis.sendCommand(Protocol.Command.SET, "a".getBytes(), "b".getBytes()))
                .asString().isEqualTo("QUEUED");
        assertThat((byte[]) jedis.sendCommand(Protocol.Command.DISCARD)).asString().isEqualTo("OK");
    }

    @TestTemplate
    public void multiIsNotAllowedInMulti(Jedis jedis) {
        jedis.sendCommand(Protocol.Command.MULTI);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.MULTI)).isInstanceOf(
                JedisDataException.class
        ).hasMessage("ERR MULTI calls can not be nested");
        jedis.sendCommand(Protocol.Command.EXEC);
    }

    @TestTemplate
    public void watchIsNotAllowedInMulti(Jedis jedis) {
        jedis.sendCommand(Protocol.Command.MULTI);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.WATCH, "foo")).isInstanceOf(
                JedisDataException.class
        ).hasMessage("ERR WATCH inside MULTI is not allowed");
        jedis.sendCommand(Protocol.Command.EXEC);
    }

    @TestTemplate
    public void flushAllIsTransactional(Jedis jedis) {
        jedis.sendCommand(Protocol.Command.MULTI);
        assertThat((byte[]) jedis.sendCommand(Protocol.Command.FLUSHALL))
                .asString().isEqualTo("QUEUED");
        assertThat((ArrayList<?>) jedis.sendCommand(Protocol.Command.EXEC)).hasSize(1);
    }

    @TestTemplate
    public void execWithoutMulti(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EXEC)).isInstanceOf(
                JedisDataException.class
        ).hasMessage("ERR EXEC without MULTI");
    }

    @TestTemplate
    public void discardTransactionOnError(Jedis jedis) {
        jedis.sendCommand(Protocol.Command.MULTI);
        jedis.sendCommand(Protocol.Command.SET, "foo", "bar");
        assertThatThrownBy(() -> jedis.sendCommand(() -> SafeEncoder.encode("no-such-command")))
                .isInstanceOf(JedisDataException.class);
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.EXEC)).isInstanceOf(
                JedisDataException.class
        ).hasMessage("EXECABORT Transaction discarded because of previous errors.");
        //Here we verify that error state was reset as well as transaction buffer
        jedis.sendCommand(Protocol.Command.MULTI);
        assertThat((ArrayList<?>) jedis.sendCommand(Protocol.Command.EXEC)).hasSize(0);
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
