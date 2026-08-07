package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToLong;

/**
 * Shared body of {@link GetRange} and {@link Substr}: one command under two
 * names, exactly as in real Redis. The only thing that distinguishes them is
 * the name reported in the arity error, which the base class already takes
 * from each subclass's own annotation.
 */
abstract class AbstractGetRange extends AbstractRedisOperation {
    AbstractGetRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected int maxArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        //Both offsets are parsed before the key is touched, so a malformed one
        //is reported ahead of WRONGTYPE or a missing key
        long start = convertToLong(params().get(1).toString());
        long end = convertToLong(params().get(2).toString());

        RMString value = base().getRMString(params().get(0));
        if (value == null) {
            //An empty bulk string, not a nil — unlike GET
            return Response.bulkString(Slice.empty());
        }
        long length = value.size();

        //Adding the length to a negative offset cannot overflow: length is
        //non-negative, so the sum only ever moves towards zero
        if (start < 0) {
            start += length;
        }
        if (end < 0) {
            end += length;
        }
        //An offset still negative after that collapses to 0 rather than to an
        //empty range, so GETRANGE key 0 -100 is the first byte and not ""
        start = Math.max(start, 0);
        end = Math.min(Math.max(end, 0), length - 1);

        if (length == 0 || start > end) {
            return Response.bulkString(Slice.empty());
        }
        //Both bounds now lie within [0, length), so the casts are safe
        return Response.bulkString(Slice.create(value.getStoredDataRange((int) start, (int) end + 1)));
    }
}
