package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RedisCommand("zpopmin")
public class ZPopMin extends AbstractByScoreOperation {
     private boolean isRev = false;

    ZPopMin(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        final Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        int count = 1;
        if (params().size() > 1) {
            String newCount = params().get(1).toString();
            count = Integer.parseInt(newCount);
        }
        List<Slice> result = new ArrayList<>();
        if (mapDBObj.isEmpty()) {
            return Response.array(result);
        }

        for (int i = 0; i < count && !mapDBObj.isEmpty(); i++) {
            ZSetEntry entry = mapDBObj.entries(isRev).first();
            result.addAll(Stream.of(entry.getValue(),
                    Slice.create(Integer.toString((int) Math.round(entry.getScore()))))
                            .map(Response::bulkString)
                    .collect(Collectors.toList()));
            mapDBObj.remove(entry.getValue());
        }
        if (mapDBObj.isEmpty()) {
            base().deleteValue(key);
        }

        return Response.array(result);
    }

    public void setRev(boolean rev) {
        isRev = rev;
    }
}
