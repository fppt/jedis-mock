package com.github.fppt.jedismock.operations.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.streams.SequencedMap;
import com.github.fppt.jedismock.datastructures.streams.StreamId;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

/**
 * XDEL key id [id ...]<br>
 */
@RedisCommand("xdel")
public class XDel extends AbstractRedisOperation {
    XDel(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        SequencedMap<StreamId, SequencedMap<Slice, Slice>> map = getStreamFromBaseOrCreateEmpty(key).getStoredData();

        List<StreamId> idsToBeDeleted = new ArrayList<>();
        try {
            for (int i = 1; i < params().size(); i++) {
                idsToBeDeleted.add(new StreamId(params().get(i)));
            }
        } catch (WrongStreamKeyException e) {
            return Response.error(e.getMessage());
        }

        int removedCount = 0;
        for (StreamId id : idsToBeDeleted) {
            if (map.remove(id) != null) {
                removedCount++;
            }
        }
        return Response.integer(removedCount);
    }
}
