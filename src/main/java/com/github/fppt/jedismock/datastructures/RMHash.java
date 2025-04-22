package com.github.fppt.jedismock.datastructures;

import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.storage.ExpiringStorage;

import java.time.Clock;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.function.Supplier;

public class RMHash extends ExpiringStorage implements RMDataStructure {
    private final LinkedHashMap<Slice, Slice> storedData = new LinkedHashMap<>();

    public RMHash(Supplier<Clock> clockSupplier) {
        super(clockSupplier, s -> {
        });
    }

    public Map<Slice, Slice> getStoredDataReadOnly() {
        storedData.entrySet().removeIf(e ->
                isKeyOutdated(e.getKey())
        );
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
        super.delete(key);
    }

    @Override
    public boolean keyExists(Slice key) {
        return verifyKey(key);
    }

    public Slice get(Slice key) {
        if (!verifyKey(key)) {
            return null;
        }
        return storedData.get(key);
    }

    public boolean isEmpty() {
        return getStoredDataReadOnly().isEmpty();
    }

    private boolean verifyKey(Slice key) {
        Objects.requireNonNull(key);
        if (!storedData.containsKey(key)) {
            return false;
        }
        if (isKeyOutdated(key)) {
            delete(key);
            return false;
        }
        return true;
    }

    /*This is needed for HLEN, which does not take into account expired keys*/
    public int sizeIncludingExpired() {
        return storedData.size();
    }

    public boolean isLazilyExpired() {
        //Check if all the fields are expired more than 1 second ago...
        if (storedData.keySet().stream().allMatch(this::isKeyOutdated)) {
            //All of them are expired, let's see how long ago...
            OptionalLong max = storedData.keySet().stream().mapToLong(this::getDeadline).max();
            if (max.isPresent() && getMillis() - max.getAsLong() > 1000) {
                storedData.clear();
                clear();
            }
        }
        return storedData.isEmpty();
    }
}
