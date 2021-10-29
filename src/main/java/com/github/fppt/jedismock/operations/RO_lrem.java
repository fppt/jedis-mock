package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import static com.github.fppt.jedismock.Utils.convertToInteger;
import static com.github.fppt.jedismock.Utils.deserializeObject;
import static com.github.fppt.jedismock.Utils.serializeObject;

@TxOperation("lrem")
class RO_lrem extends AbstractRedisOperation {
    RO_lrem(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    Slice response(){
        Slice key = params().get(0);
        int numRemove = convertToInteger(new String(params().get(1).data()));
        Slice target = params().get(2);
        Slice data = base().getValue(key);

        if(data == null){
            return Response.integer(0);
        }

        LinkedList<Slice> list = deserializeObject(data);

        //Determine the directionality of the deletions
        int numRemoved = 0;
        Iterator<Slice> iterator;
        if(numRemove < 0){
            iterator = list.descendingIterator();
        } else {
            iterator = list.iterator();
        }


        numRemove = Math.abs(numRemove);
        while (iterator.hasNext()){
            Slice element = iterator.next();
            if(element.equals(target) && (numRemove == 0 || numRemoved < numRemove)){
                iterator.remove();
                numRemoved++;
            }
        }

        base().putValue(key, serializeObject(list));

        return Response.integer(numRemoved);
    }
}
