package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

public class RMSet implements RMDataStructure, Serializable {
    private final Set<Slice> storedData;
    static final long serialVersionUID = 1L;

    public Set<Slice> getStoredData() {
        return storedData;
    }

    public RMSet() {
        storedData = new HashSet<>();
    }

    public RMSet(Set<Slice> data) {
        storedData = data;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMSet value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "set";
    }
}
