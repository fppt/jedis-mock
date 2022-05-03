package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.SerializationException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class RMHyperLogLog implements RMDataStructure, Serializable {
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
        throw new WrongValueTypeException("WRONGTYPE RMHyperLogLog value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "HyperLogLog";
    }

    @Override
    public Slice getAsSlice() {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(byteOutputStream);
            outputStream.writeObject(this);
            return Slice.create(byteOutputStream.toByteArray());
        } catch (IOException exp) {
            throw new SerializationException("problem with RMBitMap serialization");
        }
    }


}
