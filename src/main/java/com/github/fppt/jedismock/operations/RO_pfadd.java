package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

@RedisCommand("pfadd")
class RO_pfadd extends AbstractRedisOperation {
    RO_pfadd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response(){
        Slice key = params().get(0);

        RMSet dataSet = base().getSet(key);
        boolean first;

        Set<Slice> set;
        int prev;
        if (dataSet == null) {
            set = new HashSet<>();
            first = true;
            prev = 0;
        } else {
            set = dataSet.getStoredData();
            first = false;
            prev = set.size();
        }

        set.addAll(params().subList(1, params().size()));
        RMDataStructure outData = new RMSet(set);

        if (first) {
            base().putValue(key, outData);
        } else {
            base().putValue(key, outData, null);
        }

        if (prev != set.size()) {
            return Response.integer(1L);
        }
        return Response.integer(0L);
    }
}
