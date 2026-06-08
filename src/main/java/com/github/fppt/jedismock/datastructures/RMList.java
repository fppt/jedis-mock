package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.ArrayList;
import java.util.List;

public class RMList implements RMDataStructure {
    private final List<Slice> storedData;

    public RMList() {
        this.storedData = new ArrayList<>();
    }

    public RMList(List<Slice> storedData) {
        this.storedData = storedData;
    }

    public List<Slice> getStoredData() {
        return storedData;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Override
    public String getTypeName() {
        return "list";
    }
}
