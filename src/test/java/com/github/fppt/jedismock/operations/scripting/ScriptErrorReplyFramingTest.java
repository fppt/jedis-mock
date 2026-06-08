package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * A RESP error reply is a single line terminated by CRLF and must not contain a
 * bare CR or LF. A script error carried luaj's multi-line stack traceback, whose
 * embedded {@code \n} desynced strict clients (the Jedis client tolerates it, so
 * this must be checked at the raw-protocol level). See {@code Response.error} and
 * {@code Eval.stripTraceback}.
 */
class ScriptErrorReplyFramingTest {
    private RedisServer server;

    @BeforeEach
    void setUp() throws IOException {
        server = RedisServer.newRedisServer();
        server.start();
    }

    @AfterEach
    void tearDown() throws IOException {
        server.stop();
    }

    private static byte[] resp(String... parts) {
        StringBuilder sb = new StringBuilder("*").append(parts.length).append("\r\n");
        for (String p : parts) {
            sb.append("$").append(p.getBytes(StandardCharsets.UTF_8).length).append("\r\n").append(p).append("\r\n");
        }
        return sb.toString().getBytes(StandardCharsets.UTF_8);
    }

    private String roundTrip(Socket socket, String... cmd) throws IOException, InterruptedException {
        OutputStream out = socket.getOutputStream();
        InputStream in = socket.getInputStream();
        out.write(resp(cmd));
        out.flush();
        Thread.sleep(150);
        byte[] buf = new byte[8192];
        int n = in.read(buf);
        return new String(buf, 0, n, StandardCharsets.UTF_8);
    }

    private void assertSingleLineError(String reply) {
        assertThat(reply).startsWith("-").endsWith("\r\n");
        //No bare CR/LF before the single terminating CRLF.
        assertThat(reply.substring(0, reply.length() - 2)).doesNotContain("\r").doesNotContain("\n");
    }

    @Test
    void scriptErrorRepliesAreSingleLineAndDoNotDesync() throws IOException, InterruptedException {
        try (Socket socket = new Socket(server.getHost(), server.getBindPort())) {
            assertSingleLineError(roundTrip(socket, "EVAL", "redis.call('nosuchcommand')", "0"));
            assertSingleLineError(roundTrip(socket, "EVAL", "redis.call('get','a','b','c')", "0"));
            assertSingleLineError(roundTrip(socket, "EVAL", "return redis.call()", "0"));

            //Connection stays in sync: a following command is answered correctly.
            assertThat(roundTrip(socket, "PING")).isEqualTo("$4\r\nPONG\r\n");
        }
    }
}
