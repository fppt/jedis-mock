package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("zdiffstore")
class ZDiffStore extends AbstractByScoreOperation {

    ZDiffStore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice resultKey = params().get(0);
        int countKeys = Integer.parseInt(params().get(1).toString());
        if (params().size() != countKeys + 2) {
            throw new ArgumentException("*ERR*syntax*");
        }
        Slice mainKey = params().get(2);
        final RMZSet resultSet = new RMZSet();
        RMZSet startSet = getZSetFromBaseOrCreateEmpty(mainKey);
        for (ZSetEntry entry: startSet.entries(false)) {
            resultSet.put(entry.getValue(), entry.getScore());
        }

        for (int i = 0; i < countKeys - 1; i++) {
            RMZSet curSet = getZSetFromBaseOrCreateEmpty(params().get(i + 3));
            for (ZSetEntry entry: curSet.entries(false)) {
                Slice value = entry.getValue();
                if (resultSet.hasMember(value)) {
                    resultSet.remove(value);
                }
            }
        }

        base().putValue(resultKey, resultSet);
        return Response.integer(resultSet.size());
    }

}
