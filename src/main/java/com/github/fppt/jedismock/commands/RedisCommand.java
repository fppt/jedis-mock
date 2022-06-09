package com.github.fppt.jedismock.commands;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RedisCommand that = (RedisCommand) o;
        return parameters.equals(that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(parameters);
    }

    @Override
    public String toString(){
        return parameters().stream().map(Slice::toString).collect(Collectors.joining(" "));
    }

    public static RedisCommand create(){
        return new RedisCommand(new ArrayList<>());
    }
}
