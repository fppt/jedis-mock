package com.github.fppt.jedismock.datastructures;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;

public abstract class StringCompatible implements RMDataStructure, Serializable {

    @Override
    public final String getTypeName() {
        return "string";
    }

    @Override
    public final Slice getAsSlice() {
        ByteArrayOutputStream byteOutputStream = new ByteArrayOutputStream();
        try {
            ObjectOutputStream outputStream = new ObjectOutputStream(byteOutputStream);
            outputStream.writeObject(this);
            return Slice.create(byteOutputStream.toByteArray());
        } catch (IOException exp) {
            throw new IllegalStateException(exp);
        }
    }
}
