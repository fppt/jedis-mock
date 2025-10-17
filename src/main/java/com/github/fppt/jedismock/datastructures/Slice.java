package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.DeserializationException;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;

public final class Slice implements Comparable<Slice>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Slice emptySlice = new Slice(new byte[0]);
    private final byte[] storedData;


    private Slice(byte[] storedData) {
        Objects.requireNonNull(storedData);
        this.storedData = storedData;
    }

    public static Slice create(byte[] data) {
        return new Slice(data);
    }

    public static Slice create(String data) {
        return create(data.getBytes().clone());
    }

    public static Slice empty() {
        return emptySlice;
    }

    public byte[] data() {
        return storedData.clone();
    }

    public int length() {
        return storedData.length;
    }

    @Override
    public String toString() {
        return new String(storedData);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Slice)) return false;
        Slice other = (Slice) o;
        return Arrays.equals(this.storedData, other.storedData);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(storedData);
    }

    public int compareTo(Slice other) {
        Objects.requireNonNull(other, "other");
        int len1 = this.storedData.length;
        int len2 = other.storedData.length;
        int lim = Math.min(len1, len2);
        for (int i = 0; i < lim; i++) {
            int a = Byte.toUnsignedInt(this.storedData[i]);
            int b = Byte.toUnsignedInt(other.storedData[i]);
            if (a != b) {
                return Integer.compare(a, b);
            }
        }
        return Integer.compare(len1, len2);
    }

    public RMDataStructure extract() {
        if (storedData.length > 2 && storedData[0] == (byte) 0xac && storedData[1] == (byte) 0xed) {
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

        return RMString.create(storedData);
    }
}
