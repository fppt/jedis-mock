package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;

class RO_zscore extends AbstractRedisOperation {
    
    RO_zscore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Slice val = params().get(1);

        final RMHMap mapDBObj = getHMapFromBase(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();
        
        if(val == null || val.toString().isEmpty()) {
            return Response.error("Valid parameter must be provided");
        }
        
        Double score = map.get(Slice.create(val.toString()));
        
        return score == null ? Response.NULL : Response.bulkString(Slice.create(score.toString()));
    }
}
