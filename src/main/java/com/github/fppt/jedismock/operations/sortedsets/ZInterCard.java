package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;

@RedisCommand("zintercard")
class ZInterCard extends AbstractZInter {

    ZInterCard(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        List<Slice> resultArray = getResultArray();
        if (isLimit && limit != 0) {
            resultArray = resultArray.stream().limit(limit).collect(Collectors.toList());
        }
        return Response.integer(resultArray.size());
    }

}
