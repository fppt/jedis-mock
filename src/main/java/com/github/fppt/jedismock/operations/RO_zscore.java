package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@TxOperation("zscore")
class RO_zscore extends AbstractRedisOperation {
    
    RO_zscore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        Slice val = params().get(1);
        
        Map<Slice, Double> map = getDataFromBase(key, new LinkedHashMap<>());
        
        if(val == null || val.toString().isEmpty()) {
            return Response.error("Valid parameter must be provided");
        }
        
        Double score = map.get(Slice.create(val.toString()));
        
        return score == null ? Response.NULL : Response.bulkString(Slice.create(score.toString()));
    }
}
