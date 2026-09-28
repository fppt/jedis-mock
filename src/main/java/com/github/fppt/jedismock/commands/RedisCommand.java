package com.github.fppt.jedismock.commands;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public final class RedisCommand {

    private final List<Slice> parameters;

    private RedisCommand(List<Slice> parameters) {
        if (parameters == null) {
            throw new NullPointerException("Null parameters");
        }
        this.parameters = parameters;
    }

    public List<Slice> parameters() {
        return parameters;
    }

    @Override
    public String toString(){
        return parameters().stream().map(Slice::toString).collect(Collectors.joining(" "));
    }

    public static RedisCommand create(){
        return new RedisCommand(new ArrayList<>());
    }
}
