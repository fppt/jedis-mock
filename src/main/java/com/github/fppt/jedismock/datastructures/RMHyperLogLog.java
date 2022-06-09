package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;

import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

public class RMHyperLogLog extends StringCompatible {
    private static final long serialVersionUID = 1L;
    private transient HashSet<Slice> storedData;

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
        throw new WrongValueTypeException("WRONGTYPE HyperLogLog is used in the wrong place");
    }

    /* NB: we use custom serialization in order to always provide
    the same byte-wise result for the same stored data.
     */
    private void writeObject(java.io.ObjectOutputStream s)
            throws java.io.IOException {
        s.defaultWriteObject();
        s.writeInt(storedData.size());
        for (Slice storedDatum : storedData) {
            s.writeInt(storedDatum.data().length);
            for (byte datum : storedDatum.data()) {
                s.writeByte(datum);
            }
        }
    }

    private void readObject(java.io.ObjectInputStream s)
            throws java.io.IOException, ClassNotFoundException {
        storedData = new HashSet<>();
        s.defaultReadObject();
        int count = s.readInt();
        for (int i = 0; i < count; i++) {
            int len = s.readInt();
            byte[] buf = new byte[len];
            for (int j = 0; j < len; j++) {
                buf[j] = s.readByte();
            }
            storedData.add(Slice.create(buf));
        }
    }
}
