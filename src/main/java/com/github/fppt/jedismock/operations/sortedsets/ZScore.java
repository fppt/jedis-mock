package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;

@RedisCommand("zscore")
class ZScore extends AbstractRedisOperation {
    
    ZScore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Slice val = params().get(1);

        final RMHMap mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();
        
        if(val == null || val.toString().isEmpty()) {
            return Response.error("Valid parameter must be provided");
        }
        
        Double score = map.get(Slice.create(val.toString()));
        
        return score == null ? Response.NULL : Response.bulkString(Slice.create(score.toString()));
    }
}
