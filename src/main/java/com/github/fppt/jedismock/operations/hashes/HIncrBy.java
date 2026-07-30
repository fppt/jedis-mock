package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("hincrby")
class HIncrBy extends AbstractRedisOperation {
    HIncrBy(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice hsetValue(Slice key1, Slice key2, Slice value) {
        long numericValue = convertToLong(String.valueOf(value));
        Slice foundValue = base().getSlice(key1, key2);
        if (foundValue != null) {
            numericValue = Math.addExact(convertToLong(new String(foundValue.data())), numericValue);
        }
        base().putSlice(key1, key2, Slice.create(String.valueOf(numericValue)), -1L);
        return Response.integer(numericValue);
    }

    /** The event this increment reports; the float variant overrides it. */
    KeyspaceEvent incrementEvent() {
        return KeyspaceEvent.HINCRBY;
    }

    protected Slice response() {
        Slice key1 = params().get(0);
        Slice key2 = params().get(1);
        Slice value = params().get(2);
        Slice result = hsetValue(key1, key2, value);
        base().notifyKeyspaceEvent(incrementEvent(), key1);
        return result;
    }
}
