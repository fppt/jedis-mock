package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.server.Slice;
import com.google.common.collect.Lists;

import java.util.LinkedList;
import java.util.List;

@TxOperation("lpush")
class RO_lpush extends RO_add<LinkedList<Slice>> {
    RO_lpush(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    void addSliceToCollection(LinkedList<Slice> list, Slice slice) {
        list.addFirst(slice);
    }

    @Override
    LinkedList<Slice> getDefaultResponse() {
        return Lists.newLinkedList();
    }

}
