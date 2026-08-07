package com.github.fppt.jedismock.comparisontests.sets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class SInterCardTest {

    @BeforeEach
    public void setUp(Jedis jedis) {
        jedis.flushAll();
    }

    @TestTemplate
    public void wrongNumberOfArguments(Jedis jedis) {
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD))
                .hasMessage("ERR wrong number of arguments for 'sintercard' command");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "1"))
                .hasMessage("ERR wrong number of arguments for 'sintercard' command");
    }

    @TestTemplate
    public void illegalNumkeys(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "0", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "-1", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "a", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "1.5", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        //Redis' string2ll rejects a leading plus, leading zeroes and overflow
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "+1", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "01", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, " 1", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "99999999999999999999", "myset"))
                .hasMessage("ERR numkeys should be greater than 0");
    }

    @TestTemplate
    public void numkeysGreaterThanNumberOfArgs(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");
        jedis.sadd("myset2", "b", "c", "d");

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "2", "myset"))
                .hasMessage("ERR Number of keys can't be greater than number of args");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "3", "myset", "myset2"))
                .hasMessage("ERR Number of keys can't be greater than number of args");
        //A numkeys too large for an int is still just too large for the argument count
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "3000000000", "myset"))
                .hasMessage("ERR Number of keys can't be greater than number of args");
    }

    @TestTemplate
    public void syntaxErrors(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");
        jedis.sadd("myset2", "b", "c", "d");

        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "myset2"))
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "bar_arg"))
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT"))
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "1", "extra"))
                .hasMessage("ERR syntax error");
    }

    @TestTemplate
    public void illegalLimit(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");

        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "-1"))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "a"))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "1.5"))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", ""))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "+1"))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "01"))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "myset", "LIMIT", "-0"))
                .hasMessage("ERR LIMIT can't be negative");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD,
                        "1", "myset", "LIMIT", "9999999999999999999"))
                .hasMessage("ERR LIMIT can't be negative");
    }

    /**
     * Argument validation happens before any key is looked at, so a syntax
     * error beats the WRONGTYPE a bad key would otherwise produce.
     */
    @TestTemplate
    public void argumentErrorsWinOverWrongType(Jedis jedis) {
        jedis.set("key1", "x");

        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "0", "key1"))
                .hasMessage("ERR numkeys should be greater than 0");
        assertThatThrownBy(() -> jedis.sendCommand(Protocol.Command.SINTERCARD, "2", "key1"))
                .hasMessage("ERR Number of keys can't be greater than number of args");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "key1", "bar_arg"))
                .hasMessage("ERR syntax error");
        assertThatThrownBy(() ->
                jedis.sendCommand(Protocol.Command.SINTERCARD, "1", "key1", "LIMIT", "-1"))
                .hasMessage("ERR LIMIT can't be negative");
    }

    @TestTemplate
    public void againstNonSetShouldThrowError(Jedis jedis) {
        jedis.sadd("set", "a", "b", "c");
        jedis.set("key1", "x");

        assertThatThrownBy(() -> jedis.sintercard("key1"))
                .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        assertThatThrownBy(() -> jedis.sintercard("set", "key1"))
                .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        //The type of every key is checked, even once the intersection is known to be empty
        assertThatThrownBy(() -> jedis.sintercard("key1", "noset"))
                .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
        assertThatThrownBy(() -> jedis.sintercard("noset", "key1"))
                .hasMessage("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @TestTemplate
    public void againstNonExistingKey(Jedis jedis) {
        assertThat(jedis.sintercard("non-existing-key")).isEqualTo(0);
        assertThat(jedis.sintercard(0, "non-existing-key")).isEqualTo(0);
        assertThat(jedis.sintercard(10, "non-existing-key")).isEqualTo(0);
    }

    @TestTemplate
    public void withTwoSets(Jedis jedis) {
        for (int i = 0; i < 200; i++) {
            jedis.sadd("set1", String.valueOf(i));
            jedis.sadd("set2", String.valueOf(i + 195));
        }
        jedis.sadd("set1", "foo");
        jedis.sadd("set2", "foo");

        assertThat(jedis.sintercard("set1", "set2")).isEqualTo(6);
        assertThat(jedis.sintercard(0, "set1", "set2")).isEqualTo(6);
        assertThat(jedis.sintercard(3, "set1", "set2")).isEqualTo(3);
        assertThat(jedis.sintercard(10, "set1", "set2")).isEqualTo(6);
    }

    @TestTemplate
    public void againstThreeSets(Jedis jedis) {
        for (int i = 0; i < 200; i++) {
            jedis.sadd("set1", String.valueOf(i));
            jedis.sadd("set2", String.valueOf(i + 195));
        }
        jedis.sadd("set3", "199", "195", "1000", "2000");
        jedis.sadd("set1", "foo");
        jedis.sadd("set2", "foo");
        jedis.sadd("set3", "foo");

        assertThat(jedis.sintercard("set1", "set2", "set3")).isEqualTo(3);
        assertThat(jedis.sintercard(0, "set1", "set2", "set3")).isEqualTo(3);
        assertThat(jedis.sintercard(2, "set1", "set2", "set3")).isEqualTo(2);
        assertThat(jedis.sintercard(10, "set1", "set2", "set3")).isEqualTo(3);
    }

    @TestTemplate
    public void emptyIntersectionIsZeroWhateverTheLimit(Jedis jedis) {
        jedis.sadd("e1", "x");
        jedis.sadd("e2", "y");

        assertThat(jedis.sintercard("e1", "e2")).isEqualTo(0);
        assertThat(jedis.sintercard(5, "e1", "e2")).isEqualTo(0);
    }

    @TestTemplate
    public void repeatedKeyIntersectsWithItself(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");

        assertThat(jedis.sintercard("myset", "myset")).isEqualTo(3);
        assertThat(jedis.sintercard("myset", "myset", "myset")).isEqualTo(3);
        assertThat(jedis.sintercard(2, "myset", "myset")).isEqualTo(2);
    }

    @TestTemplate
    public void limitIsCaseInsensitiveAndTheLastOneWins(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");
        jedis.sadd("myset2", "a", "b", "c");

        assertThat(jedis.sendCommand(Protocol.Command.SINTERCARD,
                "2", "myset", "myset2", "LiMiT", "0")).isEqualTo(3L);
        assertThat(jedis.sendCommand(Protocol.Command.SINTERCARD,
                "2", "myset", "myset2", "LIMIT", "0", "LIMIT", "1")).isEqualTo(1L);
    }

    @TestTemplate
    public void doesNotCreateOrModifyKeys(Jedis jedis) {
        jedis.sadd("myset", "a", "b", "c");
        jedis.sadd("myset2", "b", "c", "d");

        assertThat(jedis.sintercard("myset", "myset2")).isEqualTo(2);
        assertThat(jedis.keys("*")).containsExactlyInAnyOrder("myset", "myset2");
    }
}
