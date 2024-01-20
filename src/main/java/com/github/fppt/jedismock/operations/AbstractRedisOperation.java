package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Objects;

public abstract class AbstractRedisOperation implements RedisOperation {
    private final RedisBase base;
    private final List<Slice> params;

    public AbstractRedisOperation(RedisBase base, List<Slice> params) {
        this.base = base;
        this.params = params;
    }

    protected void doOptionalWork() {
        //Place Holder For Ops which need to so some operational work
    }

    protected abstract Slice response();

    protected RedisBase base() {
        return base;
    }

    protected final List<Slice> params() {
        return params;
    }

    public RMList getListFromBaseOrCreateEmpty(Slice key) {
        RMList data = base().getList(key);
        if (data == null) {
            return new RMList();
        }

        return data;
    }

    public RMSet getSetFromBaseOrCreateEmpty(Slice key) {
        RMSet data = base().getSet(key);
        if (data == null) {
            return new RMSet();
        }

        return data;
    }

    public RMStream getStreamFromBaseOrCreateEmpty(Slice key) {
        RMStream data = base().getStream(key);
        return data == null ? new RMStream() : data;
    }

    public RMZSet getZSetFromBaseOrCreateEmpty(Slice key) {
        RMZSet data = base().getZSet(key);
        if (data == null) {
            return new RMZSet();
        }
        return data;
    }

    /***
     * Minimum number of arguments (inclusive)
     */
    protected int minArgs() {
        return 0;
    }

    /***
     * Maximum number of arguments (inclusive)
     */
    protected int maxArgs() {
        return Integer.MAX_VALUE;
    }

    @Override
    public Slice execute() {
        RedisCommand self = Objects.requireNonNull(getClass().getAnnotation(RedisCommand.class));
        try {
            //Validate the number of arguments
            if (params().size() < minArgs() || params().size() > maxArgs()) {
                return Response.error(String.format("ERR wrong number of arguments for '%s' command", self.value()));
            }
            doOptionalWork();
            return response();
        } catch (IndexOutOfBoundsException e) {
            throw new IllegalArgumentException(
                    String.format("Received wrong number of arguments when executing command [%s]", self.value()), e);
        }
    }
}
