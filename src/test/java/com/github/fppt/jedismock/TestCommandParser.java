package com.github.fppt.jedismock;

import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.exception.EOFException;
import com.github.fppt.jedismock.exception.ParseErrorException;
import com.github.fppt.jedismock.server.SliceParser;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static com.github.fppt.jedismock.commands.RedisCommandParser.parse;
import static com.github.fppt.jedismock.server.SliceParser.expectByte;
import static org.assertj.core.api.Assertions.assertThat;
import static com.github.fppt.jedismock.server.SliceParser.consumeByte;
import static com.github.fppt.jedismock.server.SliceParser.consumeCount;
import static com.github.fppt.jedismock.server.SliceParser.consumeLong;
import static com.github.fppt.jedismock.server.SliceParser.consumeParameter;
import static com.github.fppt.jedismock.server.SliceParser.consumeSlice;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestCommandParser {

    @Test
    public void testConsumeCharacter() throws ParseErrorException, EOFException {
        InputStream stream = new ByteArrayInputStream("a".getBytes());
        assertThat(consumeByte(stream)).isEqualTo((byte) 'a');
    }

    @Test
    public void testExpectCharacter() throws ParseErrorException, EOFException {
        InputStream stream = new ByteArrayInputStream("a".getBytes());
        SliceParser.expectByte(stream, (byte) 'a');
    }

    @Test
    public void testConsumeLong() throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream("12345678901234\r".getBytes());
        assertThat(consumeLong(stream)).isEqualTo(12345678901234L);
    }

    @Test
    public void testConsumeString() throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream("abcd".getBytes());
        assertThat(consumeSlice(stream, 4).toString()).isEqualTo("abcd");
    }

    @Test
    public void testConsumeCount1() throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream("*12\r\n".getBytes());
        assertThat(consumeCount(stream)).isEqualTo(12L);
    }

    @Test
    public void testConsumeCount2() {
        InputStream stream = new ByteArrayInputStream("*2\r".getBytes());
        assertThatThrownBy(() -> consumeCount(stream))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testConsumeParameter() throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream("$5\r\nabcde\r\n".getBytes());
        assertThat(consumeParameter(stream).toString()).isEqualTo("abcde");
    }

    @Test
    public void testParse() throws ParseErrorException {
        RedisCommand cmd = RedisCommandParser.parse("*3\r\n$0\r\n\r\n$4\r\nabcd\r\n$2\r\nef\r\n");
        assertThat(cmd.parameters().get(0).toString()).isEqualTo("");
        assertThat(cmd.parameters().get(1).toString()).isEqualTo("abcd");
        assertThat(cmd.parameters().get(2).toString()).isEqualTo("ef");
    }

    @Test
    public void testConsumeCharacterError() throws ParseErrorException {
        InputStream stream = new ByteArrayInputStream("".getBytes());
        assertThatThrownBy(() -> consumeByte(stream))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testExpectCharacterError1() throws EOFException {
        InputStream stream = new ByteArrayInputStream("a".getBytes());
        assertThatThrownBy(() -> expectByte(stream, (byte) 'b'))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testExpectCharacterError2() throws ParseErrorException {
        InputStream in = new InputStream() {
            @Override
            public int read() throws IOException {
                throw new IOException();
            }
        };
        assertThatThrownBy(() -> expectByte(in, (byte) 'b'))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testConsumeLongError1() {
        InputStream stream = new ByteArrayInputStream("\r".getBytes());
        assertThatThrownBy(() -> consumeLong(stream))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testConsumeLongError2() {
        InputStream stream = new ByteArrayInputStream("100a".getBytes());
        assertThatThrownBy(() -> consumeLong(stream))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testConsumeLongError3() {
        InputStream stream = new ByteArrayInputStream("".getBytes());
        assertThatThrownBy(() -> consumeLong(stream))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testConsumeStringError() {
        InputStream stream = new ByteArrayInputStream("abc".getBytes());
        assertThatThrownBy(() -> consumeSlice(stream, 4))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testConsumeCountError1() {
        InputStream stream = new ByteArrayInputStream("$12\r\n".getBytes());
        assertThatThrownBy(() -> consumeCount(stream))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testConsumeCountError2() {
        InputStream stream = new ByteArrayInputStream("*12\ra".getBytes());
        assertThatThrownBy(() -> consumeCount(stream))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testConsumeParameterError1() {
        InputStream stream = new ByteArrayInputStream("$4\r\nabcde\r\n".getBytes());
        assertThatThrownBy(() -> consumeParameter(stream))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testConsumeParameterError2() {
        InputStream stream = new ByteArrayInputStream("$4\r\nabc\r\n".getBytes());
        assertThatThrownBy(() -> consumeParameter(stream))
                .isInstanceOf(ParseErrorException.class);
    }

    @Test
    public void testConsumeParameterError3() {
        InputStream stream = new ByteArrayInputStream("$4\r\nabc".getBytes());
        assertThatThrownBy(() -> consumeParameter(stream))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testConsumeParameterError4() {
        InputStream stream = new ByteArrayInputStream("$4\r".getBytes());
        assertThatThrownBy(() -> consumeParameter(stream))
                .isInstanceOf(EOFException.class);
    }

    @Test
    public void testParseError() throws ParseErrorException {
        assertThatThrownBy(() -> parse("*0\r\n"))
                .isInstanceOf(ParseErrorException.class);
    }
}
