package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.LinkedHashMap;
import java.util.Map;

public class RMZSet implements RMDataStructure {
    private final Map<Slice, Double> storedData;

    public Map<Slice, Double> getStoredData() {
        return storedData;
    }

    public RMZSet() {
        storedData = new LinkedHashMap<>();
    }

    public RMZSet(Map<Slice, Double> data) {
        storedData = data;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMZSet value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "zset";
    }
}
