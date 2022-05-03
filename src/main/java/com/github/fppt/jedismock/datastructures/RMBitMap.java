package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.SerializationException;
import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.BitSet;

public class RMBitMap implements RMDataStructure, Serializable {
    private static final long serialVersionUID= 1L;
    private final BitSet bitSet;
    private int size;

    public RMBitMap() {
        this.size = 0;
        this.bitSet = new BitSet();
    }

    public RMBitMap(byte[] data) {
        this.size = data.length;
        this.bitSet = BitSet.valueOf(data);
    }

    public RMBitMap(int size, BitSet bitSet) {
        this.size = size;
        this.bitSet = bitSet;
    }

    public int getSize() {
        return size;
    }

    public BitSet getBitSet() {
        return bitSet;
    }

    public void setBit(byte bit, int pos) {
        int newSize = (pos + 7) / 8;

        if (size < newSize) {
            size = newSize;
        }

        bitSet.set(pos, bit == 1);
    }

    public boolean getBit(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public String getTypeName() {
        return "bitmap";
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMBitMap value is used in the wrong place");
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
