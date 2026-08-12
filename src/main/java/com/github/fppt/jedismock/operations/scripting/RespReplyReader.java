package com.github.fppt.jedismock.operations.scripting;

import java.nio.charset.StandardCharsets;

/**
 * Minimal reader for the RESP2 bytes that jedis-mock produces for its own
 * replies. Used by {@link LuaRedisCallback} to turn a command result back into
 * Lua values.
 *
 * <p>Reads from a complete in-memory reply, so unlike a network reader it never
 * has to block or refill; a truncated or malformed reply is a bug in the mock
 * rather than a transport condition, and is reported as
 * {@link IllegalStateException}.
 */
final class RespReplyReader {

    private final byte[] data;
    private int pos;

    RespReplyReader(byte[] data) {
        this.data = data;
    }

    byte readByte() {
        if (pos >= data.length) {
            throw new IllegalStateException("Truncated reply: expected another byte");
        }
        return data[pos++];
    }

    /**
     * Reads up to {@code len} bytes into {@code buf}, returning the number of
     * bytes read or -1 at the end of the input, mirroring
     * {@link java.io.InputStream#read(byte[], int, int)}.
     */
    int read(byte[] buf, int off, int len) {
        if (pos >= data.length) {
            return -1;
        }
        int read = Math.min(len, data.length - pos);
        System.arraycopy(data, pos, buf, off, read);
        pos += read;
        return read;
    }

    /**
     * Reads the bytes up to the terminating CRLF, which is consumed.
     */
    byte[] readLineBytes() {
        int start = pos;
        while (pos < data.length && data[pos] != '\r') {
            pos++;
        }
        int end = pos;
        expect((byte) '\r');
        expect((byte) '\n');
        byte[] line = new byte[end - start];
        System.arraycopy(data, start, line, 0, line.length);
        return line;
    }

    String readLine() {
        return new String(readLineBytes(), StandardCharsets.UTF_8);
    }

    int readIntCrLf() {
        return (int) readLongCrLf();
    }

    /**
     * Parses a CRLF-terminated signed decimal. RESP uses -1 lengths for null
     * bulk and null multi-bulk replies, so the sign is significant.
     */
    long readLongCrLf() {
        boolean negative = false;
        if (pos < data.length && data[pos] == '-') {
            negative = true;
            pos++;
        }
        long value = 0;
        boolean anyDigits = false;
        while (pos < data.length && data[pos] != '\r') {
            byte digit = data[pos++];
            if (digit < '0' || digit > '9') {
                throw new IllegalStateException("Malformed number in reply");
            }
            value = value * 10 + (digit - '0');
            anyDigits = true;
        }
        if (!anyDigits) {
            throw new IllegalStateException("Malformed number in reply: no digits");
        }
        expect((byte) '\r');
        expect((byte) '\n');
        return negative ? -value : value;
    }

    private void expect(byte expected) {
        if (readByte() != expected) {
            throw new IllegalStateException("Malformed reply: expected byte " + expected);
        }
    }
}
