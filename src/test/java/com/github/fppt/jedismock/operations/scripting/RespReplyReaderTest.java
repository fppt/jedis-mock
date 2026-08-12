package com.github.fppt.jedismock.operations.scripting;

import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RespReplyReaderTest {

    private static RespReplyReader reader(String resp) {
        return new RespReplyReader(resp.getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void readsStatusReplyLine() {
        RespReplyReader r = reader("+OK\r\n");
        assertThat(r.readByte()).isEqualTo((byte) '+');
        assertThat(r.readLineBytes()).isEqualTo("OK".getBytes(StandardCharsets.UTF_8));
    }

    @Test
    void readsErrorReplyLineAsString() {
        RespReplyReader r = reader("-WRONGTYPE Operation against a key holding the wrong kind of value\r\n");
        assertThat(r.readByte()).isEqualTo((byte) '-');
        assertThat(r.readLine())
                .isEqualTo("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Test
    void readsPositiveIntegerReply() {
        RespReplyReader r = reader(":42\r\n");
        assertThat(r.readByte()).isEqualTo((byte) ':');
        assertThat(r.readLongCrLf()).isEqualTo(42L);
    }

    @Test
    void readsNegativeIntegerReply() {
        RespReplyReader r = reader(":-7\r\n");
        assertThat(r.readByte()).isEqualTo((byte) ':');
        assertThat(r.readLongCrLf()).isEqualTo(-7L);
    }

    @Test
    void readsNullBulkLength() {
        RespReplyReader r = reader("$-1\r\n");
        assertThat(r.readByte()).isEqualTo((byte) '$');
        assertThat(r.readIntCrLf()).isEqualTo(-1);
    }

    @Test
    void readsNullMultiBulkLength() {
        RespReplyReader r = reader("*-1\r\n");
        assertThat(r.readByte()).isEqualTo((byte) '*');
        assertThat(r.readIntCrLf()).isEqualTo(-1);
    }

    @Test
    void readsBulkPayloadIntoBuffer() {
        RespReplyReader r = reader("$3\r\nabc\r\n");
        assertThat(r.readByte()).isEqualTo((byte) '$');
        assertThat(r.readIntCrLf()).isEqualTo(3);
        byte[] buf = new byte[3];
        assertThat(r.read(buf, 0, 3)).isEqualTo(3);
        assertThat(buf).isEqualTo("abc".getBytes(StandardCharsets.UTF_8));
        assertThat(r.readByte()).isEqualTo((byte) '\r');
        assertThat(r.readByte()).isEqualTo((byte) '\n');
    }

    @Test
    void readReturnsMinusOneAtEndOfInput() {
        RespReplyReader r = reader("");
        assertThat(r.read(new byte[4], 0, 4)).isEqualTo(-1);
    }

    @Test
    void readIsBoundedByRemainingInput() {
        RespReplyReader r = reader("ab");
        byte[] buf = new byte[8];
        assertThat(r.read(buf, 0, 8)).isEqualTo(2);
    }

    @Test
    void preservesNonAsciiBulkPayloadBytes() {
        byte[] payload = "héllo".getBytes(StandardCharsets.UTF_8);
        RespReplyReader r = reader("$" + payload.length + "\r\nhéllo\r\n");
        assertThat(r.readByte()).isEqualTo((byte) '$');
        assertThat(r.readIntCrLf()).isEqualTo(payload.length);
        byte[] buf = new byte[payload.length];
        assertThat(r.read(buf, 0, payload.length)).isEqualTo(payload.length);
        assertThat(buf).isEqualTo(payload);
    }

    @Test
    void rejectsLengthWithNoDigits() {
        RespReplyReader r = reader("$\r\n");
        r.readByte();
        assertThatThrownBy(r::readIntCrLf).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsNonNumericLength() {
        RespReplyReader r = reader("$1x\r\n");
        r.readByte();
        assertThatThrownBy(r::readIntCrLf).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsReadPastEndOfInput() {
        RespReplyReader r = reader("+");
        r.readByte();
        assertThatThrownBy(r::readByte).isInstanceOf(IllegalStateException.class);
    }

    @Test
    void rejectsLineWithoutTerminator() {
        RespReplyReader r = reader("+OK");
        r.readByte();
        assertThatThrownBy(r::readLineBytes).isInstanceOf(IllegalStateException.class);
    }
}
