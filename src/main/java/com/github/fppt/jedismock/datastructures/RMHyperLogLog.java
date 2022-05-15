package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class RMHyperLogLog extends StringCompatible {
    private static final long serialVersionUID= 1L;
    private final HashSet<Slice> storedData;

    public RMHyperLogLog() {
        storedData = new HashSet<>();
    }

    public Set<Slice> getStoredData() {
        return storedData;
    }

    public int size() {
        return storedData.size();
    }

    public void addAll(Collection<Slice> data) {
        storedData.addAll(data);
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE HyperLogLog is used in the wrong place");
    }
}
