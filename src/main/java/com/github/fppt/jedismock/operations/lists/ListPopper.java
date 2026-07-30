package com.github.fppt.jedismock.operations.lists;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

abstract class ListPopper extends AbstractRedisOperation {

    private boolean publishEvents = true;

    ListPopper(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    abstract Slice popper(List<Slice> list);

    /** The event this pop reports: {@code lpop} or {@code rpop}. */
    abstract KeyspaceEvent popEvent();

    /**
     * Suppresses this pop's own notifications, for callers that reuse it as a
     * building block and report the composite operation themselves (see
     * {@code RPOPLPUSH}, which must report the destination push first).
     */
    final void doNotPublishEvents() {
        publishEvents = false;
    }

    private Slice pop(Slice key, List<Slice> list) {
        Slice result = popper(list);
        base().markKeyModified(key);
        if (list.isEmpty()) {
            base().deleteValue(key);
        }
        return result;
    }

    /**
     * Reports one event per command — not one per element popped — followed by
     * the generic {@code del} if the list is now gone.
     */
    private void publishPop(Slice key, List<Slice> list) {
        if (!publishEvents) {
            return;
        }
        base().notifyKeyspaceEvent(popEvent(), key);
        if (list.isEmpty()) {
            base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
        }
    }

    protected final Slice response() {
        Slice key = params().get(0);
        RMList listDBObj = getListFromBaseOrCreateEmpty(key);
        List<Slice> list = listDBObj.getStoredData();

        if (list.isEmpty()) return Response.NULL;
        if (params().size() > 1) {
            //Count param
            Slice countParam = params().get(1);
            int count = Integer.parseInt(countParam.toString());
            if (count <= 0) {
                throw new WrongValueTypeException("value is out of range, must be positive");
            }
            List<Slice> responseList = new ArrayList<>();
            while (count > 0 && !list.isEmpty()) {
                responseList.add(Response.bulkString(pop(key, list)));
                count--;
            }
            publishPop(key, list);
            return Response.array(responseList);
        } else {
            Slice popped = pop(key, list);
            publishPop(key, list);
            return Response.bulkString(popped);
        }
    }

}
