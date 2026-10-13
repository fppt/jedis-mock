package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.List;

/**
 * Everything the executor needs to know about a command before it touches the
 * operation class: how to validate the call, whether it is queued in MULTI,
 * whether it writes, and how to build the operation. One instance per command,
 * created by the generated registry from the {@link RedisCommand} annotation.
 */
public final class CommandDescriptor {

    /**
     * Builds the operation for one call, supplying the constructor arguments
     * the command's constructor declares.
     */
    @FunctionalInterface
    public interface Factory {
        RedisOperation create(OperationExecutorState state, List<Slice> params);
    }

    private final String name;
    private final boolean transactional;
    private final int arity;
    private final boolean readOnly;
    private final Factory factory;

    public CommandDescriptor(String name, boolean transactional, int arity, boolean readOnly, Factory factory) {
        this.name = name;
        this.transactional = transactional;
        this.arity = arity;
        this.readOnly = readOnly;
        this.factory = factory;
    }

    public String name() {
        return name;
    }

    /** See {@link RedisCommand#transactional()}. */
    public boolean transactional() {
        return transactional;
    }

    /** See {@link RedisCommand#arity()}. */
    public int arity() {
        return arity;
    }

    /** See {@link RedisCommand#readOnly()}. */
    public boolean readOnly() {
        return readOnly;
    }

    /**
     * Rejects a call whose argument count does not satisfy {@link #arity()},
     * with Redis' own error message. Like Redis, this runs at dispatch, before
     * the operation is built, run, or queued inside MULTI.
     *
     * @param argCount the number of arguments, excluding the command name
     */
    public void validateArity(int argCount) {
        int argc = argCount + 1;
        if (arity > 0 ? argc != arity : argc < -arity) {
            throw new IllegalArgumentException(
                    String.format("ERR wrong number of arguments for '%s' command", name));
        }
    }

    public RedisOperation instantiate(OperationExecutorState state, List<Slice> params) {
        return factory.create(state, params);
    }
}
