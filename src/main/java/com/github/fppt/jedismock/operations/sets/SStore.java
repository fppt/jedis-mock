package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;
import java.util.function.BiFunction;

public abstract class SStore extends AbstractRedisOperation {
    private final BiFunction<RedisBase, List<Slice>, Set<Slice>> operation;

    @Override
    protected int minArgs() {
        return 2;
    }

    public SStore(RedisBase base,
                  List<Slice> params,
                  BiFunction<RedisBase, List<Slice>, Set<Slice>> operation) {
        super(base, params);
        this.operation = operation;
    }

    /** The event this store reports, e.g. {@code sinterstore}. */
    abstract KeyspaceEvent storeEvent();

    @Override
    protected final Slice response() {
        Slice key = params().get(0);
        boolean destinationExisted = base().exists(key);
        Set<Slice> result = operation.apply(base(), params().subList(1, params().size()));
        if (result.isEmpty()) {
            base().deleteValue(key);
            //An empty result removes the destination, reported as a generic del
            if (destinationExisted) {
                base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
            }
        } else {
            base().putValue(key, new RMSet(result));
            base().notifyKeyspaceEvent(storeEvent(), key);
        }
        return Response.integer(result.size());
    }
}
