package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.EOFException;
import com.github.fppt.jedismock.exception.ParseErrorException;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.io.ByteArrayDataOutput;
import com.google.common.io.ByteStreams;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

public class SliceParser {

    @VisibleForTesting
    public static byte consumeByte(InputStream messageInput) throws EOFException {
        int b;
        try {
            b = messageInput.read();
        } catch (IOException e) {
            throw new EOFException();
        }
        if (b == -1) {
            throw new EOFException();
        }
        return (byte) b;
    }

    @VisibleForTesting
    public static void expectByte(InputStream messageInput, byte c) throws ParseErrorException, EOFException {
        if (consumeByte(messageInput) != c) {
            throw new ParseErrorException();
        }
    }

    @VisibleForTesting
    public static long consumeLong(InputStream messageInput) throws ParseErrorException {
        byte c;
        long ret = 0;
        boolean hasLong = false;
        while (true) {
            try {
                c = consumeByte(messageInput);
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
            if (c == '\r') {
                break;
            }
            if (!isNumber(c)) {
                throw new ParseErrorException();
            }
            ret = ret * 10 + c - '0';
            hasLong = true;
        }
        if (!hasLong) {
            throw new ParseErrorException();
        }
        return ret;
    }

    @VisibleForTesting
    public static Slice consumeSlice(InputStream messageInput, long len) throws ParseErrorException {
        ByteArrayDataOutput bo = ByteStreams.newDataOutput();
        for (long i = 0; i < len; i++) {
            try {
                bo.write(consumeByte(messageInput));
            } catch (EOFException e) {
                throw new ParseErrorException();
            }
        }
        return Slice.create(bo.toByteArray());
    }

    @VisibleForTesting
    public static long consumeCount(InputStream messageInput) throws ParseErrorException {
        try {
            expectByte(messageInput, (byte) '*');
            long count = consumeLong(messageInput);
            expectByte(messageInput, (byte) '\n');
            return count;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    public static long consumeCount(byte[] message) throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream(message);
        return consumeCount(stream);
    }

    public static int consumeInteger(byte[] message) throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream(message);
        return consumeInteger(stream);
    }

    public static int consumeInteger(InputStream messageInput) throws ParseErrorException {
        try {
            expectByte(messageInput, (byte) ':');
            long count = consumeLong(messageInput);
            return (int) count;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    private static boolean isNumber(byte c) {
        return '0' <= c && c <= '9';
    }

    public static Slice consumeParameter(InputStream messageInput) throws ParseErrorException {
        try {
            expectByte(messageInput, (byte) '$');
            long len = consumeLong(messageInput);
            expectByte(messageInput, (byte) '\n');
            Slice para = consumeSlice(messageInput, len);
            expectByte(messageInput, (byte) '\r');
            expectByte(messageInput, (byte) '\n');
            return para;
        } catch (EOFException e) {
            throw new ParseErrorException();
        }
    }

    public static Slice consumeParameter(byte[] message) throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream(message);
        return consumeParameter(stream);
    }
}
