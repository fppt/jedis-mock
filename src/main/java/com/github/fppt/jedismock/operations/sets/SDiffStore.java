package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;


@RedisCommand("sdiffstore")
class SDiffStore extends SStore {
    SDiffStore(RedisBase base, List<Slice> params) {
        super(base, params,
                (b, p) -> new SDiff(b, p).getDifference());
    }

    @Override
    KeyspaceEvent storeEvent() {
        return KeyspaceEvent.SDIFFSTORE;
    }
}
