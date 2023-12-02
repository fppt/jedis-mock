package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.ArrayList;
import java.util.List;
import java.util.NavigableSet;

import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.BYLEX;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.BYSCORE;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.LIMIT;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.REV;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.WITHSCORES;

@RedisCommand("zrangestore")
class ZRangeStore extends AbstractZRangeByIndex {
    private final Object lock;

    ZRangeStore(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    @Override
    protected Slice response() {
        if (options.contains(WITHSCORES)) {
            throw new ArgumentException("*syntax*");
        }
        Slice keyDest = params().get(0);
        params().remove(0);
        key = params().get(0);
        if (!base().exists(key)) {
            base().deleteValue(keyDest);
            return Response.integer(0);
        }
        mapDBObj = base().getZSet(key);

        if (options.contains(BYSCORE) && !options.contains(REV)) {
            ZRangeByScore zRangeByScore = new ZRangeByScore(base(), new ArrayList<>());
            zRangeByScore.key = key;
            zRangeByScore.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRangeByScore.getRange(zRangeByScore.getStartBound(params().get(1)), zRangeByScore.getEndBound(params().get(2))));
        }
        if (options.contains(BYSCORE)) {
            ZRevRangeByScore zRevRangeByScore = new ZRevRangeByScore(base(), new ArrayList<>());
            zRevRangeByScore.key = key;
            zRevRangeByScore.mapDBObj = mapDBObj;
            zRevRangeByScore.options.add(REV);
            return saveToNewKey(keyDest, zRevRangeByScore.getRange(zRevRangeByScore.getStartBound(params().get(2)), zRevRangeByScore.getEndBound(params().get(1))));
        }
        if (options.contains(BYLEX) && !options.contains(REV)) {
            ZRangeByLex zRangeByLex = new ZRangeByLex(base(), new ArrayList<>());
            zRangeByLex.key = key;
            zRangeByLex.mapDBObj = mapDBObj;
            return saveToNewKey(keyDest, zRangeByLex.getRange(zRangeByLex.getStartBound(params().get(1)), zRangeByLex.getEndBound(params().get(2))));
        }
        if (options.contains(BYLEX)) {
            ZRevRangeByLex zRevRangeByLex = new ZRevRangeByLex(base(), new ArrayList<>());
            zRevRangeByLex.key = key;
            zRevRangeByLex.mapDBObj = mapDBObj;
            zRevRangeByLex.options.add(REV);
            return saveToNewKey(keyDest, zRevRangeByLex.getRange(zRevRangeByLex.getStartBound(params().get(2)), zRevRangeByLex.getEndBound(params().get(1))));
        }
        if (options.contains(LIMIT)) {
            throw new ArgumentException("ERR syntax error, LIMIT is only supported in combination with either BYSCORE or BYLEX");
        }
        if (checkWrongIndex()) {
            base().deleteValue(keyDest);
            return Response.integer(0);
        }

        NavigableSet<ZSetEntry> entries = getRange(getStartBound(Slice.create(String.valueOf(startIndex))), getStartBound(Slice.create(String.valueOf(endIndex))));

        return saveToNewKey(keyDest, entries);
    }

    private Slice saveToNewKey(Slice keyDest, NavigableSet<ZSetEntry> entries) {
        RMZSet resultZSet = new RMZSet();
        if (options.contains(LIMIT)) {
            int tempOffset = 0;
            int tempCount = 0;
            for (ZSetEntry entry : entries) {
                if (tempOffset < offset) {
                    tempOffset++;
                    continue;
                }
                if (count == -1) {
                    resultZSet.put(entry.getValue(), entry.getScore());
                } else if (tempCount < count) {
                    resultZSet.put(entry.getValue(), entry.getScore());
                    tempCount++;
                } else {
                    break;
                }
            }
        } else {
            for (ZSetEntry entry : entries) {
                resultZSet.put(entry.getValue(), entry.getScore());
            }
        }
        base().deleteValue(keyDest);
        if (resultZSet.size() > 0) {
            base().putValue(keyDest, resultZSet);
            lock.notifyAll();
        }
        return Response.integer(resultZSet.size());
    }
}
