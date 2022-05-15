package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;

public class ExpiringKeyValueStorage {
    private final Map<Slice, RMDataStructure> values = new HashMap<>();
    private final Map<Slice, Long> ttls = new HashMap<>();
    private final Consumer<Slice> keyChangeNotifier;

    public ExpiringKeyValueStorage(Consumer<Slice> keyChangeNotifier) {
        this.keyChangeNotifier = keyChangeNotifier;
    }

    public Map<Slice, RMDataStructure> values() {
        return values;
    }

    public Map<Slice, Long> ttls()  {
        return ttls;
    }

    public void delete(Slice key) {
        keyChangeNotifier.accept(key);
        ttls().remove(key);
        values().remove(key);
    }

    public void delete(Slice key1, Slice key2) {
        keyChangeNotifier.accept(key1);
        Objects.requireNonNull(key2);

        if (!verifyKey(key1)) {
            return;
        }
        RMHash sortedSetByKey = getRMSortedSet(key1);
        Map<Slice, Slice> storedData = sortedSetByKey.getStoredData();

        if (!storedData.containsKey(key2)) {
            return;
        }

        storedData.remove(key2);

        if(storedData.isEmpty()) {
            values.remove(key1);
        }

        if (!values().containsKey(key1)) {
            ttls().remove(key1);
        }
    }

    public void clear() {
        values().clear();
        ttls().clear();
    }

    public RMDataStructure getValue(Slice key) {
        if(!verifyKey(key)) {
            return null;
        }
        return values().get(key);
    }

    private boolean verifyKey(Slice key) {
        Objects.requireNonNull(key);
        if(!values().containsKey(key)) {
            return false;
        }

        if (isKeyOutdated(key)) {
            delete(key);
            return false;
        }
        return true;
    }

    private boolean isKeyOutdated(Slice key) {
        Long deadline = ttls().get(key);
        return deadline != null && deadline != -1 && deadline <= System.currentTimeMillis();
    }

    public Long getTTL(Slice key) {
        Objects.requireNonNull(key);
        Long deadline = ttls().get(key);
        if (deadline == null) {
            return null;
        }
        if (deadline == -1) {
            return deadline;
        }
        long now = System.currentTimeMillis();
        if (now < deadline) {
            return deadline - now;
        }
        delete(key);
        return null;
    }

    public long setTTL(Slice key, long ttl) {
        keyChangeNotifier.accept(key);
        return setDeadline(key, ttl + System.currentTimeMillis());
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

        if(!values.containsKey(key1)) {
            mapByKey = new RMHash();
            values.put(key1, mapByKey);
        } else {
            mapByKey = getRMSortedSet(key1);
        }
        mapByKey.put(key2, value);
        configureTTL(key1, ttl);
    }

    private RMHash getRMSortedSet(Slice key) {
        RMDataStructure valueByKey = values.get(key);
        if(!isSortedSetValue(valueByKey)) {
            valueByKey.raiseTypeCastException();
        }

        return (RMHash) valueByKey;
    }

    private void configureTTL(Slice key, Long ttl) {
        if (ttl == null) {
            // If a TTL hasn't been provided, we don't want to override the TTL. However, if no TTL is set for this key,
            // we should still set it to -1L
            if (getTTL(key) == null) {
                setDeadline(key, -1L);
            }
        } else {
            if (ttl != -1) {
                setTTL(key, ttl);
            } else {
                setDeadline(key, -1L);
            }
        }
    }

    public long setDeadline(Slice key, long deadline) {
        Objects.requireNonNull(key);
        if (values().containsKey(key)) {
            ttls().put(key, deadline);
            return 1L;
        }
        return 0L;
    }

    public boolean exists(Slice slice) {
        return verifyKey(slice);
    }

    private boolean isSortedSetValue(RMDataStructure value) {
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
