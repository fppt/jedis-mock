package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("spop")
class SPop extends AbstractRedisOperation {
    SPop(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private List<Slice> popper(Set<Slice> collection, int numberToBeRemoved) {
        List<Slice> result = new ArrayList<>();
        Iterator<Slice> it = collection.iterator();

        while (numberToBeRemoved-- > 0 && it.hasNext()) {
            result.add(Response.bulkString(it.next()));
            it.remove();
        }

        return result;
    }

    protected Slice response() {
        Slice key = params().get(0);
        Set<Slice> set = getSetFromBaseOrCreateEmpty(key).getStoredData();

        int numberOfElementsToBeRemoved = params().size() > 1
                ? convertToInteger(params().get(1).toString())
                : 1;

        if (set.isEmpty()) {
            return params().size() > 1 ? Response.EMPTY_ARRAY : Response.NULL;
        }

        List<Slice> removedElements = popper(set, numberOfElementsToBeRemoved);

        if (set.isEmpty()) {
            base().deleteValue(key);
        }

        if (params().size() > 1) {
            return Response.array(removedElements);
        } else {
            return removedElements.get(0);
        }
    }
}
