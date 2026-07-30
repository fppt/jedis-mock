package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;


@RedisCommand("sinterstore")
class SInterStore extends SStore {
    SInterStore(RedisBase base, List<Slice> params) {
        super(base, params,
                (b, p) -> new SInter(b, p).getIntersection());
    }

    @Override
    KeyspaceEvent storeEvent() {
        return KeyspaceEvent.SINTERSTORE;
    }
}
