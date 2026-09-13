package com.github.fppt.jedismock.comparisontests.connection;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.util.SafeEncoder;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class HelloTest {

    @TestTemplate
    public void helloReturnsTheServerHandshakeMap(Jedis jedis) {
        Map<String, Object> hello = hello(jedis, "2");

        assertThat(hello).containsOnlyKeys("server", "version", "proto", "id", "mode", "role", "modules");
        assertThat(hello).containsEntry("server", "redis");
        assertThat(hello).containsEntry("mode", "standalone");
        assertThat(hello).containsEntry("role", "master");
        //The comparison container ships bundled modules, so only the type is comparable.
        assertThat(hello.get("modules")).isInstanceOf(List.class);
        //The protocol version must come back as a RESP integer: Jedis 8 casts it to Long.
        assertThat(hello).containsEntry("proto", 2L);
        assertThat((String) hello.get("version")).matches("\\d+\\.\\d+\\.\\d+");
        assertThat((Long) hello.get("id")).isPositive();
    }

    @TestTemplate
    public void helloWithoutArgumentsKeepsResp2(Jedis jedis) {
        assertThat(hello(jedis)).containsEntry("proto", 2L);
    }

    @TestTemplate
    public void helloAssignsADistinctIdPerConnection(Jedis jedis, HostAndPort hostAndPort) {
        long id = (Long) hello(jedis, "2").get("id");
        try (Jedis other = new Jedis(hostAndPort)) {
            assertThat((Long) hello(other, "2").get("id")).isNotEqualTo(id);
        }
    }

    @TestTemplate
    public void helloSetsTheConnectionName(Jedis jedis) {
        try {
            assertThat(hello(jedis, "2", "SETNAME", "hello-name")).containsEntry("proto", 2L);
            assertThat(jedis.clientGetname()).isEqualTo("hello-name");
        } finally {
            jedis.clientSetname("");
        }
    }

    @TestTemplate
    public void helloDoesNotSetTheConnectionNameWhenALaterOptionIsInvalid(Jedis jedis) {
        try {
            assertThatThrownBy(() -> hello(jedis, "2", "SETNAME", "hello-name", "BAD"))
                    .isInstanceOf(JedisDataException.class)
                    .hasMessage("ERR Syntax error in HELLO option 'BAD'");
            assertThat(jedis.clientGetname()).isNull();
        } finally {
            jedis.clientSetname("");
        }
    }

    @TestTemplate
    public void helloAcceptsInlineAuthForTheNopassDefaultUser(Jedis jedis) {
        assertThat(hello(jedis, "2", "AUTH", "default", "whatever")).containsEntry("proto", 2L);
    }

    @TestTemplate
    public void helloRejectsAnUnsupportedProtocolVersion(Jedis jedis) {
        assertThatThrownBy(() -> hello(jedis, "4"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("NOPROTO unsupported protocol version");
        assertThatThrownBy(() -> hello(jedis, "1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("NOPROTO unsupported protocol version");
        assertThatThrownBy(() -> hello(jedis, "0"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("NOPROTO unsupported protocol version");
        assertThatThrownBy(() -> hello(jedis, "-1"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("NOPROTO unsupported protocol version");
    }

    @TestTemplate
    public void helloRejectsANonIntegerProtocolVersion(Jedis jedis) {
        assertThatThrownBy(() -> hello(jedis, "abc"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Protocol version is not an integer or out of range");
        assertThatThrownBy(() -> hello(jedis, "3.0"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Protocol version is not an integer or out of range");
        assertThatThrownBy(() -> hello(jedis, ""))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Protocol version is not an integer or out of range");
        assertThatThrownBy(() -> hello(jedis, "99999999999999999999"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Protocol version is not an integer or out of range");
    }

    @TestTemplate
    public void helloRejectsAnUnknownOption(Jedis jedis) {
        assertThatThrownBy(() -> hello(jedis, "2", "FOO"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Syntax error in HELLO option 'FOO'");
    }

    @TestTemplate
    public void helloRejectsAnIncompleteOption(Jedis jedis) {
        assertThatThrownBy(() -> hello(jedis, "2", "AUTH"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Syntax error in HELLO option 'AUTH'");
        assertThatThrownBy(() -> hello(jedis, "2", "AUTH", "default"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Syntax error in HELLO option 'AUTH'");
        assertThatThrownBy(() -> hello(jedis, "2", "SETNAME"))
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR Syntax error in HELLO option 'SETNAME'");
    }

    /**
     * Sends HELLO and flattens the RESP2 reply (a flat array of alternating
     * field names and values) into a map, decoding only the bulk strings so
     * that the RESP type of every other value is preserved.
     */
    @SuppressWarnings("unchecked")
    private static Map<String, Object> hello(Jedis jedis, String... args) {
        List<Object> reply = (List<Object>) jedis.sendCommand(Protocol.Command.HELLO, args);
        Map<String, Object> result = new LinkedHashMap<>();
        for (int i = 0; i + 1 < reply.size(); i += 2) {
            result.put(SafeEncoder.encode((byte[]) reply.get(i)), decode(reply.get(i + 1)));
        }
        return result;
    }

    private static Object decode(Object value) {
        return value instanceof byte[] ? SafeEncoder.encode((byte[]) value) : value;
    }
}
