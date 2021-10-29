package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.HashSet;
import java.util.Set;

import static com.github.fppt.jedismock.Utils.deserializeObject;

public class RMSet implements RMDataStructure {
    protected Set<Slice> storedData;

    public Set<Slice> getStoredData() {
        return storedData;
    }

    public RMSet(Slice data) {
        if (data == null) {
            storedData = new HashSet<>();
            return;
        }
        try {
            storedData = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize HashSet<Slice> value");
        }
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMSet value is used in the wrong place");
    }
}
