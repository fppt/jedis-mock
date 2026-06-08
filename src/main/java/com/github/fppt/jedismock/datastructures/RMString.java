package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class RMString implements RMDataStructure, Serializable {
    private static final long serialVersionUID = 1L;
    private byte[] storedData;

    public RMString() {
        storedData = new byte[0];
    }

    public RMString(byte[] data) {
        storedData = data;
    }

    public byte[] getStoredData() {
        return Arrays.copyOf(storedData, storedData.length);
    }

    public String getStoredDataAsString() {
        return new String(storedData, StandardCharsets.UTF_8);
    }

    public static RMString create(byte[] str) {
        return new RMString(str);
    }

    public static RMString create(String str) {
        return new RMString(str.getBytes(StandardCharsets.UTF_8));
    }

    public void add(byte[] str) {
        int oldLength = storedData.length;
        storedData = Arrays.copyOf(storedData, oldLength + str.length);
        System.arraycopy(str, 0, storedData, oldLength, str.length);
    }

    public int size() {
        return storedData.length;
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE Operation against a key holding the wrong kind of value");
    }

    @Override
    public String getTypeName() {
        return "string";
    }

    @Override
    public Slice getAsSlice() {
        return Slice.create(storedData);
    }
}
