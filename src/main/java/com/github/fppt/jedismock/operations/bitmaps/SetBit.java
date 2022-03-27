package com.github.fppt.jedismock.operations.bitmaps;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.BitSet;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToByte;
import static com.github.fppt.jedismock.Utils.convertToNonNegativeInteger;

@RedisCommand("setbit")
class SetBit extends AbstractRedisOperation {
    SetBit(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice value = base().getSlice(params().get(0));
        byte bit = convertToByte(params().get(2).toString());
        int pos = convertToNonNegativeInteger(params().get(1).toString());

        if (value == null) {
            BitSet bs = BitSet.valueOf(new byte[]{0});
            if (bit == 1) {
                bs.set(pos, true);
                base().putSlice(params().get(0), Slice.create(bs.toByteArray()));
            } else {
                base().putSlice(params().get(0), Slice.create(new byte[(pos / 8) + 1]));
            }

            return Response.integer(0);
        }

        BitSet bs = BitSet.valueOf(value.data());
        int res = bs.get(pos) ? 1 : 0;
        bs.set(pos, bit == 1);
        boolean size = value.data().length >= bs.toByteArray().length;
        byte[] data = new byte[size ? value.data().length : (pos / 8) + 1];
        System.arraycopy(bs.toByteArray(), 0, data, 0, bs.toByteArray().length);
        base().putSlice(params().get(0), Slice.create(data));
        return Response.integer(res);
    }
}
