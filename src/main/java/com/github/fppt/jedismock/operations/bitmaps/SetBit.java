package com.github.fppt.jedismock.operations.bitmaps;

import com.github.fppt.jedismock.datastructures.RMBitMap;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToByte;
import static com.github.fppt.jedismock.Utils.convertToNonNegativeInteger;

@RedisCommand("setbit")
class SetBit extends AbstractRedisOperation {
    SetBit(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        RMBitMap value = base().getBitMap(key);
        byte bit = convertToByte(params().get(2).toString());
        int pos = convertToNonNegativeInteger(params().get(1).toString());

        if (value == null) {
            RMBitMap bitMap = new RMBitMap();
            bitMap.setBit(bit, pos);
            base().putValue(key, bitMap);

            return Response.integer(0);
        }

        boolean res = value.getBit(pos);
        value.setBit(bit, pos);
        base().putValue(key, value);
        return Response.integer(res ? 1 : 0);
    }
}
