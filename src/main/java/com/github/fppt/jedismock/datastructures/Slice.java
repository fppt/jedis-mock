package com.github.fppt.jedismock.datastructures;


import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.Serializable;
import java.util.Arrays;

public class Slice implements RMDataStructure, Comparable<Slice>, Serializable {
    private static final long serialVersionUID = 247772234876073528L;
    private final byte[] data;

    private Slice(byte[] data) {
        if (data == null) {
            throw new NullPointerException("Null data");
        }
        this.data = data;
    }

    public static Slice create(byte[] data){
        return new Slice(data);
    }

    public static Slice create(String data){
        return create(data.getBytes().clone());
    }

    public byte[] data() {
        return Arrays.copyOf(data, data.length);
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
