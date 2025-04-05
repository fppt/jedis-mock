package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.storage.ExpiringStorage;

import java.time.Clock;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

public class RMHash extends ExpiringStorage implements RMDataStructure {
    private final LinkedHashMap<Slice, Slice> storedData = new LinkedHashMap<>();

    public RMHash(Supplier<Clock> clockSupplier) {
        super(clockSupplier, s -> {
        });
    }

    public Map<Slice, Slice> getStoredDataReadOnly() {
        return Collections.unmodifiableMap(storedData);
    }

    public void put(Slice key, Slice data) {
        storedData.put(key, data);
    }

    @Override
    public void raiseTypeCastException() {
        throw new WrongValueTypeException("WRONGTYPE RMSortedSet value is used in the wrong place");
    }

    @Override
    public String getTypeName() {
        return "hash";
    }

    @Override
    public void delete(Slice key) {
        storedData.remove(key);
    }

    @Override
    public boolean keyExists(Slice key) {
        return storedData.containsKey(key);
    }

    public Slice get(Slice key) {
        return storedData.get(key);
    }

    public boolean isEmpty() {
        return storedData.isEmpty();
    }
}
