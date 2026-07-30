package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;


@RedisCommand("sunionstore")
class SUnionStore extends SStore {
    SUnionStore(RedisBase base, List<Slice> params) {
        super(base, params,
                (b, p) -> new SUnion(b, p).getUnion());
    }

    @Override
    KeyspaceEvent storeEvent() {
        return KeyspaceEvent.SUNIONSTORE;
    }
}
