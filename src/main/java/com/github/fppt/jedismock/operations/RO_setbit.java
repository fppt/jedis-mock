package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Arrays;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToByte;
import static com.github.fppt.jedismock.Utils.convertToNonNegativeInteger;

@RedisCommand("setbit")
class RO_setbit extends AbstractRedisOperation {
    RO_setbit(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response() {
        Slice value = base().getSlice(params().get(0));

        byte bit = convertToByte(params().get(2).toString());
        int pos = convertToNonNegativeInteger(params().get(1).toString());

        if (value == null) {
            byte[] data = new byte[pos / 8 + 1];
            Arrays.fill(data, (byte) 0);
            data[pos / 8] = (byte) (bit << (pos % 8));
            base().putSlice(params().get(0), Slice.create(data));
            return Response.integer(0L);
        }

        long original;
        if (pos / 8 >= value.length()) {
            byte[] data = new byte[pos / 8 + 1];
            Arrays.fill(data, (byte) 0);
            for (int i = 0; i < value.length(); i++) {
                data[i] = value.data()[i];
            }
            data[pos / 8] = (byte) (bit << (pos % 8));
            original = 0;
            base().putSlice(params().get(0), Slice.create(data));
        } else {
            byte[] data = value.data();
            if ((data[pos / 8] & (1 << (pos % 8))) != 0) {
                original = 1;
            } else {
                original = 0;
            }
            data[pos / 8] |= (byte) (1 << (pos % 8));
            data[pos / 8] &= (byte) (bit << (pos % 8));
            base().putSlice(params().get(0), Slice.create(data));
        }
        return Response.integer(original);
    }
}
