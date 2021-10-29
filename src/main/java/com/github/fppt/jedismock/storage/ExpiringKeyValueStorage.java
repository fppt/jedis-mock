package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMSortedSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.google.common.base.Preconditions;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ExpiringKeyValueStorage {
    private final Map<Slice, RMDataStructure> values = new HashMap<>();
    private final Map<Slice, Long> ttls = new HashMap<>();

    public Map<Slice, RMDataStructure> values() {
        return values;
    }

    public Map<Slice, Long> ttls()  {
        return ttls;
    }

    public void delete(Slice key) {
        ttls().remove(key);
        values().remove(key);
    }

    public void delete(Slice key1, Slice key2) {
        Preconditions.checkNotNull(key2);

        if (!verifyKey(key1)) {
            return;
        }
        RMSortedSet sortedSetByKey = getRMSortedSet(key1);
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

    public Slice getSlice(Slice key) {
        if(!verifyKey(key)) {
            return null;
        }
        RMDataStructure valueByKey = values.get(key);
        if(!(valueByKey instanceof Slice)) {
            valueByKey.raiseTypeCastException();
        }

        return (Slice) valueByKey;
    }

    public Map<Slice, Slice> getFieldsAndValues(Slice hash) {
        if(!verifyKey(hash)) {
            return new HashMap<>();
        }
        return getRMSortedSet(hash).getStoredData();
    }

    public Slice getSlice(Slice key1, Slice key2) {
        Preconditions.checkNotNull(key2);

        if(!verifyKey(key1)) {
            return null;
        };
        RMSortedSet sortedSet = getRMSortedSet(key1);

        if(!sortedSet.getStoredData().containsKey(key2)) {
            return null;
        }
        return sortedSet.getStoredData().get(key2);
    }

    private boolean verifyKey(Slice key) {
        Preconditions.checkNotNull(key);

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
        Preconditions.checkNotNull(key);

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
        return setDeadline(key, ttl + System.currentTimeMillis());
    }

    public void put(Slice key, RMDataStructure value, Long ttl) {
        values().put(key, value);
        configureTTL(key, ttl);
    }

        // Put inside
    public void put(Slice key, Slice value, Long ttl) {
        Preconditions.checkNotNull(key);
        Preconditions.checkNotNull(value);
        values().put(key, value);
        configureTTL(key, ttl);
    }

    // Put into inner RMHMap
    public void put(Slice key1, Slice key2, Slice value, Long ttl) {
        Preconditions.checkNotNull(key1);
        Preconditions.checkNotNull(key2);
        Preconditions.checkNotNull(value);
        RMSortedSet mapByKey = null;
        if(!values.containsKey(key1)) {
            mapByKey = new RMSortedSet(null);
            values.put(key1, mapByKey);
        } else {
            mapByKey = getRMSortedSet(key1);
        }
        mapByKey.put(key2, value);
        configureTTL(key1, ttl);
    }

    private RMSortedSet getRMSortedSet(Slice key) {
        RMDataStructure valueByKey = values.get(key);
        if(!isSortedSetValue(valueByKey)) {
            valueByKey.raiseTypeCastException();
        }

        return (RMSortedSet) valueByKey;
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
        Preconditions.checkNotNull(key);

        if (values().containsKey(key)) {
            ttls().put(key, deadline);
            return 1L;
        }
        return 0L;
    }

    public boolean exists(Slice slice) {
        if (values().containsKey(slice)) {
            Long deadline = ttls().get(slice);
            if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
                delete(slice);
                return false;
            } else {
                return true;
            }
        }
        return false;
    }

    private boolean isSortedSetValue(RMDataStructure value) {
        return value instanceof RMSortedSet;
    }

    public Slice type(Slice key) {
        //We also check for ttl here
        if (!verifyKey(key)) {
            return Slice.create("none");
        }

        RMDataStructure valueByKey = getValue(key);

        if (valueByKey == null) {
            return Slice.create("hash");
        }

        if(isSortedSetValue(valueByKey)) {
            return Slice.create("hash");
        }

        Slice value = (Slice) valueByKey;

        //0xACED is a magic number denoting a serialized Java object
        if (value.data()[0] == (byte) 0xAC
                && value.data()[1] == (byte) 0xED){
            Object o = Utils.deserializeObject(value);
            if (o instanceof List){
                return Slice.create("list");
            }
            if (o instanceof Set){
                return Slice.create("set");
            }
            if (o instanceof Map){
                return Slice.create("zset");
            }
            throw new IllegalStateException("Unknown value type");
        }
        return Slice.create("string");
    }
}
