package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

public abstract class AbstractRedisOperation implements RedisOperation {
    private final RedisBase base;
    private final List<Slice> params;

    public AbstractRedisOperation(RedisBase base, List<Slice> params) {
        this.base = base;
        this.params = params;
    }

    protected void doOptionalWork(){
        //Place Holder For Ops which need to so some operational work
    }

    protected abstract Slice response();

    protected RedisBase base(){
        return base;
    }

    protected final List<Slice> params() {
        return params;
    }

    public RMList getListFromBaseOrCreateEmpty(Slice key) {
        RMList data = base().getList(key);
        if(data == null) {
            return new RMList();
        }

        return data;
    }

    public RMSet getSetFromBaseOrCreateEmpty(Slice key) {
        RMSet data = base().getSet(key);
        if(data == null) {
            return new RMSet();
        }

        return data;
    }

    public RMZSet getZSetFromBaseOrCreateEmpty(Slice key) {
        RMZSet data = base().getZSet(key);
        if(data == null) {
            return new RMZSet();
        }
        return data;
    }

    @Override
    public Slice execute(){
        try {
            doOptionalWork();
            return response();
        } catch (IndexOutOfBoundsException e){
            throw new IllegalArgumentException("Invalid number of arguments when executing command [" + getClass().getSimpleName() + "]", e);
        }
    }
}
