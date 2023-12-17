package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zlexcount")
class ZLexCount extends AbstractZRangeByLex {

    ZLexCount(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        expectNoOptions();
        key = params().get(0);
        mapDBObj = getZSetFromBaseOrCreateEmpty(key);

        Slice start = params().get(1);
        if (notValidate(start.toString())) {
            return buildErrorResponse("start");
        }

        Slice end = params().get(2);
        if (notValidate(end.toString())) {
            return buildErrorResponse("end");
        }

        return Response.integer(getRange(getStartBound(start), getEndBound(end)).size());
    }

    private boolean notValidate(String forValidate) {
        return !NEGATIVELY_INFINITE.equals(forValidate) && !POSITIVELY_INFINITE.equals(forValidate) &&
                !startsWithAnyPrefix(forValidate);
    }
}
