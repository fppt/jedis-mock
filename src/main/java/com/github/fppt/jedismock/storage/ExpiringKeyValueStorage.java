package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public class ExpiringKeyValueStorage extends ExpiringStorage {
    private final Map<Slice, RMDataStructure> values = new HashMap<>();

    public ExpiringKeyValueStorage(Supplier<Clock> clockSupplier, Consumer<Slice> keyChangeNotifier) {
        super(clockSupplier, keyChangeNotifier);
    }

    public Map<Slice, RMDataStructure> values() {
        return values;
    }

    @Override
    public void delete(Slice key) {
        keyChangeNotifier.accept(key);
        super.delete(key);
        values().remove(key);
    }

    @Override
    protected boolean keyExists(Slice key) {
        return values.containsKey(key);
    }

    public void delete(Slice key1, Slice key2) {
        keyChangeNotifier.accept(key1);
        Objects.requireNonNull(key2);

        if (!verifyKey(key1)) {
            return;
        }
        RMHash hashByKey = getRMHash(key1);
        if (!hashByKey.keyExists(key2)) {
            return;
        }
        hashByKey.delete(key2);

        if (hashByKey.isEmpty()) {
            values.remove(key1);
        }

        if (!values().containsKey(key1)) {
            super.delete(key1);
        }
    }

    @Override
    public void clear() {
        for (Slice key : values().keySet()) {
            if (!isKeyOutdated(key)) {
                keyChangeNotifier.accept(key);
            }
        }
        values().clear();
        super.clear();
    }

    public RMDataStructure getValue(Slice key) {
        if (!verifyKey(key)) {
            return null;
        }
        return values().get(key);
    }

    private boolean verifyKey(Slice key) {
        Objects.requireNonNull(key);
        if (!values().containsKey(key)) {
            return false;
        }

        if (isKeyOutdated(key)) {
            delete(key);
            return false;
        }

        RMDataStructure o = values.get(key);
        //Do not actively expire at this point, but if it's empty -- then clean it up
        if (o instanceof RMHash && ((RMHash) o).isLazilyExpired()) {
            delete(key);
            return false;
        }
        return true;
    }

    public void put(Slice key, RMDataStructure value, Long ttl) {
        keyChangeNotifier.accept(key);
        values().put(key, value);
        configureTTL(key, ttl);
    }

    // Put inside
    public void put(Slice key, Slice value, Long ttl) {
        keyChangeNotifier.accept(key);
        Objects.requireNonNull(key);
        Objects.requireNonNull(value);
        values().put(key, value.extract());
        configureTTL(key, ttl);
    }

    // Put into inner RMHMap
    public void put(Slice key1, Slice key2, Slice value, Long ttl) {
        keyChangeNotifier.accept(key1);
        Objects.requireNonNull(key1);
        Objects.requireNonNull(key2);
        Objects.requireNonNull(value);
        RMHash mapByKey;

        if (!values.containsKey(key1)) {
            mapByKey = new RMHash(getClockSupplier());
            values.put(key1, mapByKey);
        } else {
            mapByKey = getRMHash(key1);
        }
        mapByKey.put(key2, value);
        configureTTL(key1, ttl);
    }

    private RMHash getRMHash(Slice key) {
        RMDataStructure valueByKey = values.get(key);
        if (!isHashValue(valueByKey)) {
            valueByKey.raiseTypeCastException();
        }

        return (RMHash) valueByKey;
    }

    public boolean exists(Slice slice) {
        return verifyKey(slice);
    }

    private boolean isHashValue(RMDataStructure value) {
        return value instanceof RMHash;
    }

    public Slice type(Slice key) {
        //We also check for ttl here
        if (!verifyKey(key)) {
            return Slice.create("none");
        }
        RMDataStructure valueByKey = getValue(key);

        if (valueByKey == null) {
            return Slice.create("none");
        }
        return Slice.create(valueByKey.getTypeName());
    }
}
