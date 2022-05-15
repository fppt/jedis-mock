package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.DeserializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;

public class Slice implements Comparable<Slice>, Serializable {
    private static final long serialVersionUID= 1L;
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

    public RMDataStructure extract() {
        if (storedData[0] == (byte) 0xac && storedData[1] == (byte) 0xed) {
            try {
                ObjectInputStream objectInputStream = new ObjectInputStream(new ByteArrayInputStream(storedData));
                Object value = objectInputStream.readObject();

                if (value instanceof RMDataStructure) {
                    return (RMDataStructure) value;
                }

            } catch (IOException | ClassNotFoundException ex) {
                throw new DeserializationException("problems with deserialization");
            }
        }

        return RMString.create(this.toString());
    }
}
