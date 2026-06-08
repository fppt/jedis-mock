package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public class RMSet implements RMDataStructure {
    private final Set<Slice> storedData;

    public RMSet() {
        storedData = new HashSet<>();
    }

    public RMSet(Set<Slice> data) {
        Objects.requireNonNull(data);
        storedData = data;
    }

    public Set<Slice> getStoredData() {
        return storedData;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Override
    public String getTypeName() {
        return "set";
    }
}
