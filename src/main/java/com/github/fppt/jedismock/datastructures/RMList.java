package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import static com.github.fppt.jedismock.Utils.deserializeObject;

import java.util.LinkedList;
import java.util.List;

public class RMList implements RMDataStructure {
    protected List<Slice> storedData;

    public List<Slice> getStoredData() {
        return storedData;
    }

    public RMList(Slice data) {
        if (data == null) {
            storedData = new LinkedList<>();
            return;
        }
        try {
            storedData = deserializeObject(data);
        } catch (WrongValueTypeException e) {
            throw new WrongValueTypeException("WRONGTYPE Failed to deserialize LinkedList<Slice> value");
        }
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMList value is used in the wrong place");
    }
}
