package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Collection;
import java.util.List;

import static com.github.fppt.jedismock.Utils.serializeObject;

abstract class RO_pop<V extends Collection<Slice>> extends AbstractRedisOperation {
    RO_pop(RedisBase base, List<Slice> params ) {
        super(base, params);
    }

    abstract Slice popper(V list);

    abstract V getDataFromBase(Slice key);

    Slice response() {
        Slice key = params().get(0);
        V collection = getDataFromBase(key);
        if(collection == null || collection.isEmpty()) return Response.NULL;
        Slice v = popper(collection);
        base().putSlice(key, serializeObject(collection));
        return Response.bulkString(v);
    }
}
