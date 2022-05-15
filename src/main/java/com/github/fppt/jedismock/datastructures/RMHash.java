package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import java.util.LinkedHashMap;
import java.util.Map;

public class RMHash implements RMDataStructure {
    private final LinkedHashMap<Slice, Slice> storedData;

    public Map<Slice, Slice> getStoredData() {
        return storedData;
    }

    public RMHash() {
        storedData = new LinkedHashMap<>();
    }

    public void put(Slice key, Slice data) {
        storedData.put(key, data);
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMSortedSet value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "hash";
    }
}
