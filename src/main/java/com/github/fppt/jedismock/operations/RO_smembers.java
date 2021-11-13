package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import static java.util.stream.Collectors.toList;

@RedisCommand("smembers")
class RO_smembers extends AbstractRedisOperation {
    RO_smembers(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        final Slice key = params().get(0);
        final RMSet setDBObj = getSetFromBaseOrCreateEmpty(key);
        final Set<Slice> data = setDBObj.getStoredData();
        //Has to be a list because Jedis can only deserialize lists
        LinkedList<Slice> list;
        if (data != null) {
            list = new LinkedList<>(data);
        } else {
            list = new LinkedList<>();
        }

        return Response.array(list.stream().map(Response::bulkString).collect(toList()));
    }
}
