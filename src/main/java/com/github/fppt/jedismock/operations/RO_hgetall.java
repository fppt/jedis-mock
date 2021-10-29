package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@TxOperation("hgetall")
public class RO_hgetall extends AbstractRedisOperation {
    public RO_hgetall(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice hash = params().get(0);

        Map<Slice, Slice> fieldAndValueMap = base().getFieldsAndValues(hash);
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
