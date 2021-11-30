package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RedisCommand("hvals")
public class HVals extends AbstractRedisOperation {
    public HVals(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice hash = params().get(0);

        Map<Slice, Slice> fieldAndValueMap = base().getFieldsAndValues(hash);
        int arraySize = fieldAndValueMap.size();
        Slice [] fvals = new Slice[arraySize];

        int currentIndex = 0;
        for (Map.Entry<Slice, Slice> entry: fieldAndValueMap.entrySet()){
            fvals[currentIndex] = Response.bulkString(entry.getValue());
            currentIndex++;
        }

        List<Slice> values = Arrays.asList(fvals);
        return Response.array(values);
    }
}
