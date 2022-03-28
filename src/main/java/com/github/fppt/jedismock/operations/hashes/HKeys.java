package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RedisCommand("hkeys")
public class HKeys extends AbstractRedisOperation {
    public HKeys(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice hash = params().get(0);

        Map<Slice, Slice> fieldAndValueMap = base().getFieldsAndValues(hash);

        int arraySize = fieldAndValueMap.size();
        Slice [] fkeys = new Slice[arraySize];

        int currentIndex = 0;
        for (Map.Entry<Slice, Slice> entry: fieldAndValueMap.entrySet()){
            fkeys[currentIndex] = Response.bulkString(entry.getKey());
            currentIndex++;
        }

        List<Slice> values = Arrays.asList(fkeys);
        return Response.array(values);
    }
}
