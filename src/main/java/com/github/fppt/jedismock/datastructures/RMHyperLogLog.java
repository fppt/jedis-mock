package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.Set;

public class RMHLL implements RMDataStructure {
    private Set<Slice> data;


    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMHLL value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return null;
    }

    @Override
    public Slice getAsSlice() {
        return RMDataStructure.super.getAsSlice();
    }


}
