package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Set;

@RedisCommand("srem")
class SRem extends AbstractRedisOperation {


    SRem(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    final int remove() {
        Slice key = params().get(0);
        RMSet setDBObj = getSetFromBaseOrCreateEmpty(key);
        Set<Slice> set = setDBObj.getStoredData();
        if (set == null) {
            return 0;
        }
        int count = 0;
        for (int i = 1; i < params().size(); i++) {
            if (set.remove(params().get(i))) {
                count++;
            }
        }
        if (set.isEmpty()) {
            base().deleteValue(key);
        }
        return count;
    }

    protected Slice response() {
        int removed = remove();
        if (removed > 0) {
            publishRemoval(base(), params().get(0));
        }
        return Response.integer(removed);
    }

    /**
     * Reports {@code srem}, plus the generic {@code del} if that emptied the
     * set. Shared with {@code SMOVE}, which reuses {@link #remove()} directly
     * and so has to report the removal itself.
     */
    static void publishRemoval(RedisBase base, Slice key) {
        base.notifyKeyspaceEvent(KeyspaceEvent.SREM, key);
        if (!base.exists(key)) {
            base.notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
        }
    }
}
