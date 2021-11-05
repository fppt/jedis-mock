package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.deserializeObject;
import static java.util.stream.Collectors.toList;

@RedisCommand("sinter")
class RO_sinter extends AbstractRedisOperation {
    RO_sinter(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Set<Slice> resultSoFar = getSet(key);

        for (int i = 1; i < params().size(); i++) {
            Set<Slice> set = getSet(params().get(i));
            resultSoFar.retainAll(set);
        }

        return Response.array(resultSoFar.stream().map(Response::bulkString).collect(toList()));
    }

    private Set<Slice> getSet(Slice key) {
        Set<Slice> set;
        Slice data = base().getSlice(key);
        if (data != null) {
            set = new HashSet<>(deserializeObject(data));
        } else {
            set = new HashSet<>();
        }
        return set;
    }
}
