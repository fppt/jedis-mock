package com.github.fppt.jedismock.comparisontests.server;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * {@code notify-keyspace-events} is not a verbatim string parameter: Redis
 * parses it into a set of event-class flags and {@code CONFIG GET} reports
 * the canonical form (the {@code A} alias collapses the classes it covers,
 * and the characters are emitted in a fixed order).
 */
@ExtendWith(ComparisonBase.class)
public class NotifyKeyspaceEventsConfigTest {

    private static final String PARAM = "notify-keyspace-events";

    @TestTemplate
    public void flagsAreNormalizedOnRead(Jedis jedis) {
        String[][] inputToCanonical = {
                {"KA", "AK"},
                {"EA", "AE"},
                {"KEA", "AKE"},
                {"AKE", "AKE"},
                {"A", "A"},
                {"gKE", "gKE"},
                {"$lshzxeKE", "$lshzxeKE"},
                {"En", "nE"},
                {"Kn", "nK"},
                {"nKE", "nKE"},
                {"mKE", "KEm"},
                {"dKE", "dKE"},
                {"Egx", "gxE"},
                {"K", "K"},
                {"", ""},
        };
        for (String[] testCase : inputToCanonical) {
            assertThat(jedis.configSet(PARAM, testCase[0])).isEqualTo("OK");
            assertThat(jedis.configGet(PARAM))
                    .as("canonical form of '%s'", testCase[0])
                    .containsEntry(PARAM, testCase[1]);
        }
    }

    @TestTemplate
    public void invalidEventClassCharacterIsRejected(Jedis jedis) {
        jedis.configSet(PARAM, "gKE");
        for (String invalid : new String[]{"Q", "Egq", "K E"}) {
            assertThatThrownBy(() -> jedis.configSet(PARAM, invalid))
                    .as("flags '%s'", invalid)
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage("ERR CONFIG SET failed (possibly related to argument '" + PARAM
                            + "') - Invalid event class character. Use 'Ag$lshzxeKEtmdn'.");
        }
        //A rejected SET leaves the previous value in place
        assertThat(jedis.configGet(PARAM)).containsEntry(PARAM, "gKE");
        jedis.configSet(PARAM, "");
    }
}
