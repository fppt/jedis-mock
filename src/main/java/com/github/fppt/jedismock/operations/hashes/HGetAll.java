package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RedisCommand("hgetall")
public class HGetAll extends AbstractRedisOperation {
    public HGetAll(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice hash = params().get(0);

        Map<Slice, Slice> fieldAndValueMap = base().getFieldsAndValues(hash);

        if(fieldAndValueMap == null) {
            fieldAndValueMap = new HashMap<>();
        }
        int arraySize = fieldAndValueMap.size() * 2;
        Slice [] fieldAndValueList = new Slice[arraySize];

        int currentIndex = arraySize - 1;
        for (Map.Entry<Slice, Slice> entry: fieldAndValueMap.entrySet()){
            fieldAndValueList[currentIndex] = Response.bulkString(entry.getValue());
            currentIndex--;

            fieldAndValueList[currentIndex] = Response.bulkString(entry.getKey());
            currentIndex--;
        }

        List<Slice> values = Arrays.asList(fieldAndValueList);
        return Response.array(values);
    }
}
