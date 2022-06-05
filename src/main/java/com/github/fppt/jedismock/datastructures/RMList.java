package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.ArrayList;
import java.util.List;

public class RMList implements RMDataStructure {
    private final List<Slice> storedData;

    public List<Slice> getStoredData() {
        return storedData;
    }

    public RMList() {
        storedData = new ArrayList<>();
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMList value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "list";
    }
}
