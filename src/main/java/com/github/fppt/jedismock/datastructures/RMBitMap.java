package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.BitSet;

public class RMBitMap extends StringCompatible {
    private final BitSet bitSet;
    private int size;

    public RMBitMap() {
        this.size = 0;
        this.bitSet = new BitSet();
    }

    public RMBitMap(byte[] data) {
        this.size = data.length;
        byte[] reversed = new byte[data.length];
        for (int i = 0; i < data.length; i++) {
            reversed[i] = reverseBits(data[i]);
        }
        this.bitSet = BitSet.valueOf(reversed);
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
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMBitMap value is used in the wrong place");
    }

    @Override
    public final Slice getAsSlice() {
        return Slice.create(toByteArray());
    }

    /**
     * Produce bitewise representation of the bit set.
     */
    private byte[] toByteArray() {
        long[] longs = bitSet.toLongArray();
        byte[] bytes = new byte[size];
        int byteIndex = 0;
        for (long value : longs) {
            for (int i = 0; i < 8 && byteIndex < size; i++) {
                bytes[byteIndex++] = reverseBits((byte) (value & 0xFF));
                value >>>= 8;
            }
        }
        return bytes;
    }

    private static byte reverseBits(byte b) {
        int buf = b & 0xFF;
        buf = ((buf & 0xF0) >>> 4) | ((buf & 0x0F) << 4);
        buf = ((buf & 0xCC) >>> 2) | ((buf & 0x33) << 2);
        buf = ((buf & 0xAA) >>> 1) | ((buf & 0x55) << 1);
        return (byte) buf;
    }
}
