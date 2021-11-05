package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.util.List;


abstract class AbstractRedisOperation implements RedisOperation {
    private final RedisBase base;
    private final List<Slice> params;

    AbstractRedisOperation(RedisBase base, List<Slice> params) {
        this.base = base;
        this.params = params;
    }

    void doOptionalWork(){
        //Place Holder For Ops which need to so some operational work
    }

    abstract Slice response() throws IOException;

    RedisBase base(){
        return base;
    }

    List<Slice> params(){
        return params;
    }

    public RMList getListFromBase(Slice key) {
        Slice data = base().getSlice(key);
        return new RMList(data);
    }

    public RMSet getSetFromBase(Slice key) {
        Slice data = base().getSlice(key);
        return new RMSet(data);
    }

    public RMHMap getHMapFromBase(Slice key) {
        Slice data = base.getSlice(key);
        return new RMHMap(data);
    }

    @Override
    public Slice execute(){
        try {
            doOptionalWork();
            return response();
        } catch (IndexOutOfBoundsException | IOException e){
            throw new IllegalArgumentException("Invalid number of arguments when executing command [" + getClass().getSimpleName() + "]", e);
        }
    }
}
