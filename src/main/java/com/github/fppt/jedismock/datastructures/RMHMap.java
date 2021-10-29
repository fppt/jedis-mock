package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.LinkedHashMap;
import java.util.Map;

import static com.github.fppt.jedismock.Utils.deserializeObject;

public class RMHMap implements RMDataStructure {
    protected Map<Slice, Double> storedData;

    public Map<Slice, Double> getStoredData() {
        return storedData;
    }

    public RMHMap(Slice data) {
        if (data == null) {
            storedData = new LinkedHashMap<>();
            return;
        }
        try {
            storedData = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize LinkedHashMap<Slice, Double> value");
        }
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMHMap value is used in the wrong place");
    }
}
