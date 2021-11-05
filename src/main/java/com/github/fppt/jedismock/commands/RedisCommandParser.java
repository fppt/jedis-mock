package com.github.fppt.jedismock.commands;

import com.github.fppt.jedismock.server.SliceParser;
import com.github.fppt.jedismock.exception.ParseErrorException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Objects;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisCommandParser {

    public static RedisCommand parse(String stringInput) throws ParseErrorException {
        Objects.requireNonNull(stringInput);
        return parse(new ByteArrayInputStream(stringInput.getBytes()));
    }

    public static RedisCommand parse(InputStream messageInput) throws ParseErrorException {
        Objects.requireNonNull(messageInput);
        long count = SliceParser.consumeCount(messageInput);
        if (count == 0) {
            throw new ParseErrorException();
        }
        RedisCommand command = RedisCommand.create();
        for (long i = 0; i < count; i++) {
            command.parameters().add(SliceParser.consumeParameter(messageInput));
        }
        return command;
    }
}
