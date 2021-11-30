package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.ListIterator;

import static com.github.fppt.jedismock.Utils.convertToInteger;

@RedisCommand("lrem")
class LRem extends AbstractRedisOperation {
    private final int directedNumRemove;
    private final Slice target;

    private boolean isDeletingElement(Slice element, int numRemoved) {
        return element.equals(target) && (directedNumRemove == 0 || numRemoved < Math.abs(directedNumRemove));
    }

    LRem(RedisBase base, List<Slice> params) {
        super(base, params);
        directedNumRemove = convertToInteger(new String(params().get(1).data()));
        target = params().get(2);
    }

    protected Slice response(){
        Slice key = params().get(0);
        RMList listObj = base().getList(key);
        if(listObj == null){
            return Response.integer(0);
        }

        List<Slice> list = listObj.getStoredData();

        //Determine the directionality of the deletions
        int numRemoved = 0;
        ListIterator<Slice> iterator;
        if (directedNumRemove < 0) {
            iterator = list.listIterator(list.size());
        } else {
            iterator = list.listIterator();
        }

        while (directedNumRemove < 0 ? iterator.hasPrevious() : iterator.hasNext()) {
            Slice element = directedNumRemove < 0 ? iterator.previous() : iterator.next();
            if (isDeletingElement(element, numRemoved)) {
                iterator.remove();
                numRemoved++;
            }
        }
        return Response.integer(numRemoved);
    }
}
