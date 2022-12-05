package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/*
 * Syntax:
 *
 * LTRIM key start stop
 *
 * Trim an existing list so that it will contain only the specified range of elements specified.
 * Both start and stop are zero-based indexes, where 0 is the first element
 * of the list (the head), 1 the next element and so on.
 *
 * https://redis.io/commands/ltrim/
 *
 */

@RedisCommand("ltrim")
@SuppressWarnings("all")
class LTrim extends AbstractRedisOperation {
    LTrim(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        Slice key = params().get(0);
        int start = Integer.parseInt(params().get(1).toString());
        int end = Integer.parseInt(params().get(2).toString());

        final RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        final List<Slice> list = listDBObj.getStoredData();

        int size = list.size();

        // start and end can also be negative numbers indicating offsets from the end of the list,
        // where -1 is the last element of the list, -2 the penultimate element and so on.
        start = (start < 0) ? size + start : start;
        start = Math.max(start, 0);
        end = (end < 0) ? size + end : end;

        // Out of range indexes will not produce an error:
        // if start is larger than the end of the list, or start > end,
        // the result will be an empty list (which causes key to be removed).
        if (start > size || start > end || end < 0) {
            list.clear();
        } else {
            end = (end >= size) ? size : end + 1;
            list.subList(end, list.size()).clear();
            list.subList(0, start).clear();
        }

        return Response.OK;
    }
}
