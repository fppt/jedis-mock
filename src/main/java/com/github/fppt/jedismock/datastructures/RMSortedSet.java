package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import java.util.LinkedHashMap;
import static com.github.fppt.jedismock.Utils.deserializeObject;

public class RMSortedSet implements RMDataStructure {
    protected LinkedHashMap<Slice, Slice> storedData;

    public LinkedHashMap<Slice, Slice> getStoredData() {
        return storedData;
    }

    public RMSortedSet(Slice data) {
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

    public void put(Slice key, Slice data) {
        storedData.put(key, data);
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMSortedSet value is used in the wrong place");
    }
}
