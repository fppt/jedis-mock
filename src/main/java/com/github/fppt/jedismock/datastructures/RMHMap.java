package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.LinkedHashMap;
import java.util.Map;

public class RMHMap implements RMDataStructure {
    private final Map<Slice, Double> storedData;

    public Map<Slice, Double> getStoredData() {
        return storedData;
    }

    public RMHMap() {
        storedData = new LinkedHashMap<>();
    }

    public RMHMap(Map<Slice, Double> data) {
        storedData = data;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMHMap value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "zset";
    }
}
