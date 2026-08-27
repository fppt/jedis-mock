package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class ZPop extends AbstractByScoreOperation {
     private final boolean isRev;

    ZPop(RedisBase base, List<Slice> params, boolean isRev) {
        super(base, params);
        this.isRev = isRev;
    }

    @Override
    protected Slice response() {
        return Response.array(pop());
    }

    protected List<Slice> pop() {
        int count = 1;
        if (params().size() > 1) {
            String newCount = params().get(1).toString();
            count = Integer.parseInt(newCount);
        }
        if (count < 0) {
            throw new ArgumentException("ERR value is out of range, must be positive");
        }

        final Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        List<Slice> result = new ArrayList<>();
        if (mapDBObj.isEmpty()) {
            return result;
        }

        for (int i = 0; i < count && !mapDBObj.isEmpty(); i++) {
            ZSetEntry entry = mapDBObj.entries(isRev).first();
            result.addAll(Stream.of(entry.getValue(),
                            Slice.create(String.format("%.0f", entry.getScore())))
                    .map(Response::bulkString)
                    .collect(Collectors.toList()));
            mapDBObj.remove(entry.getValue());
        }
        //One event per command, and only when something was actually removed:
        //a count of 0 modifies nothing and so is not reported
        if (!result.isEmpty()) {
            base().notifyKeyspaceEvent(isRev ? KeyspaceEvent.ZPOPMAX : KeyspaceEvent.ZPOPMIN, key);
            if (mapDBObj.isEmpty()) {
                base().deleteValue(key);
                base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
            }
        }
        return result;
    }
}
