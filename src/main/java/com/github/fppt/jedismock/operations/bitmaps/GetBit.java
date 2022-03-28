package com.github.fppt.jedismock.operations.bitmaps;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.BitSet;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToNonNegativeInteger;

@RedisCommand("getbit")
class GetBit extends AbstractRedisOperation {
    GetBit(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice value = base().getSlice(params().get(0));
        int pos = convertToNonNegativeInteger(params().get(1).toString());

        if (value == null) {
            return Response.integer(0L);
        }
        BitSet bitSet = BitSet.valueOf(value.data());
        return Response.integer(bitSet.get(pos) ? 1 : 0);
    }
}
