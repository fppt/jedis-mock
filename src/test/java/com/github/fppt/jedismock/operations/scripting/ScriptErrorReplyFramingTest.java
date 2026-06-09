package com.github.fppt.jedismock.operations.scripting;

import com.github.fppt.jedismock.RedisServer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayOutputStream;
import java.io.EOFException;
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
    //Fail fast if the server never answers, instead of blocking the suite.
    private static final int READ_TIMEOUT_MS = 2_000;

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

    private String roundTrip(Socket socket, String... cmd) throws IOException {
        socket.setSoTimeout(READ_TIMEOUT_MS);
        OutputStream out = socket.getOutputStream();
        out.write(resp(cmd));
        out.flush();
        //Read back exactly one RESP reply (no fixed sleep, no guessed buffer size):
        //a desync or a malformed multi-line reply then surfaces either as bad
        //framing here or as the wrong bytes on the next reply.
        return readReply(socket.getInputStream());
    }

    /**
     * Reads exactly one RESP reply and returns its raw bytes (type byte, header
     * line and, for a bulk string, its payload) as text. Handles the reply kinds
     * this test exercises: simple string ({@code +}), error ({@code -}), integer
     * ({@code :}) and bulk string ({@code $}).
     */
    private static String readReply(InputStream in) throws IOException {
        ByteArrayOutputStream reply = new ByteArrayOutputStream();
        int type = readByte(in);
        reply.write(type);
        String header = readLine(in, reply);
        if (type == '$') {
            int length = Integer.parseInt(header);
            if (length >= 0) {
                readExactly(in, reply, length + 2); // payload + trailing CRLF
            }
        }
        return new String(reply.toByteArray(), StandardCharsets.UTF_8);
    }

    private static int readByte(InputStream in) throws IOException {
        int b = in.read();
        if (b < 0) {
            throw new EOFException("server closed the connection");
        }
        return b;
    }

    /** Reads up to (and including) the terminating CRLF, mirroring raw bytes into
     * {@code sink}; returns the line content without the CRLF. */
    private static String readLine(InputStream in, ByteArrayOutputStream sink) throws IOException {
        StringBuilder line = new StringBuilder();
        while (true) {
            int b = readByte(in);
            sink.write(b);
            if (b == '\r') {
                sink.write(readByte(in)); // consume the '\n'
                return line.toString();
            }
            line.append((char) b);
        }
    }

    private static void readExactly(InputStream in, ByteArrayOutputStream sink, int n) throws IOException {
        for (int i = 0; i < n; i++) {
            sink.write(readByte(in));
        }
    }

    private void assertSingleLineError(String reply) {
        assertThat(reply).startsWith("-").endsWith("\r\n");
        //No bare CR/LF before the single terminating CRLF.
        assertThat(reply.substring(0, reply.length() - 2)).doesNotContain("\r").doesNotContain("\n");
    }

    @Test
    void scriptErrorRepliesAreSingleLineAndDoNotDesync() throws IOException {
        try (Socket socket = new Socket(server.getHost(), server.getBindPort())) {
            assertSingleLineError(roundTrip(socket, "EVAL", "redis.call('nosuchcommand')", "0"));
            assertSingleLineError(roundTrip(socket, "EVAL", "redis.call('get','a','b','c')", "0"));
            assertSingleLineError(roundTrip(socket, "EVAL", "return redis.call()", "0"));

            //Connection stays in sync: a following command is answered correctly.
            assertThat(roundTrip(socket, "PING")).isEqualTo("$4\r\nPONG\r\n");
        }
    }
}
