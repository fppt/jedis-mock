package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Serializable;

public class RMString implements RMDataStructure, Serializable {
    private static final long serialVersionUID= 1L;
    private String storedData;

    public RMString() {
        storedData = "";
    }

    public RMString(String data) {
        storedData = data;
    }

    public String getStoredData() {
        return storedData;
    }

    public static RMString create(String str) {
        return  new RMString(str);
    }

    public void add(String str) {
        storedData = storedData + str;
    }

    public int size() {
        return storedData.length();
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMString value is used in the wrong place");
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
