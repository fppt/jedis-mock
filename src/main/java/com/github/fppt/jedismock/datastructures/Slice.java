package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.Arrays;

public class Slice implements RMDataStructure, Comparable<Slice> {
    private final byte[] storedData;

    private Slice(byte[] storedData) {
        if (storedData == null) {
            throw new NullPointerException("Null data");
        }
        this.storedData = storedData;
    }

    public static Slice create(byte[] data){
        return new Slice(data);
    }

    public static Slice create(String data){
        return create(data.getBytes().clone());
    }

    public byte[] data() {
        return Arrays.copyOf(storedData, storedData.length);
    }

    public int length(){
        return data().length;
    }

    @Override
    public String toString() {
        return new String(data());
    }

    @Override
    public boolean equals(Object b) {
        return b instanceof Slice && Arrays.equals(data(), ((Slice) b).data());
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(data());
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE Slice value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "string";
    }

    public int compareTo(Slice b) {
        int len1 = data().length;
        int len2 = b.data().length;
        int lim = Math.min(len1, len2);

        int k = 0;
        while (k < lim) {
            byte b1 = data()[k];
            byte b2 = b.data()[k];
            if (b1 != b2) {
                return b1 - b2;
            }
            k++;
        }
        return len1 - len2;
    }
}
