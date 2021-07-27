package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.server.Slice;
import com.google.auto.value.AutoValue;
import com.google.common.base.Preconditions;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Maps;
import com.google.common.collect.Table;

import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 * Used to represent an expiring storage layer.
 */
@AutoValue
public abstract class ExpiringKeyValueStorage {
    public abstract Table<Slice, Slice, Slice> values();

    public abstract Map<Slice, Long> ttls();

    public static ExpiringKeyValueStorage create() {
        return new AutoValue_ExpiringKeyValueStorage(HashBasedTable.create(), Maps.newHashMap());
    }

    public void delete(Slice key) {
        ttls().remove(key);
        values().row(key).clear();
    }

    public void delete(Slice key1, Slice key2) {
        Preconditions.checkNotNull(key1);
        Preconditions.checkNotNull(key2);
        values().remove(key1, key2);

        if (!values().containsRow(key1)) {
            ttls().remove(key1);
        }
    }

    public void clear() {
        values().clear();
        ttls().clear();
    }

    public Slice get(Slice key) {
        return get(key, Slice.reserved());
    }

    public Map<Slice, Slice> getFieldsAndValues(Slice hash) {
        return values().row(hash);
    }

    public Slice get(Slice key1, Slice key2) {
        Preconditions.checkNotNull(key1);
        Preconditions.checkNotNull(key2);

        Long deadline = ttls().get(key1);
        if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
            delete(key1);
            return null;
        }
        return values().get(key1, key2);
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

    public void put(Slice key, Slice value, Long ttl) {
        put(key, Slice.reserved(), value, ttl);
    }

    public void put(Slice key1, Slice key2, Slice value, Long ttl) {
        Preconditions.checkNotNull(key1);
        Preconditions.checkNotNull(key2);
        Preconditions.checkNotNull(value);

        values().put(key1, key2, value);
        if (ttl == null) {
            // If a TTL hasn't been provided, we don't want to override the TTL. However, if no TTL is set for this key,
            // we should still set it to -1L
            if (getTTL(key1) == null) {
                setDeadline(key1, -1L);
            }
        } else {
            if (ttl != -1) {
                setTTL(key1, ttl);
            } else {
                setDeadline(key1, -1L);
            }
        }
    }

    public long setDeadline(Slice key, long deadline) {
        Preconditions.checkNotNull(key);

        if (values().containsRow(key)) {
            ttls().put(key, deadline);
            return 1L;
        }
        return 0L;
    }

    public boolean exists(Slice slice) {
        if (values().containsRow(slice)) {
            Long deadline = ttls().get(slice);
            if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
                delete(slice, Slice.reserved());
                return false;
            } else {
                return true;
            }
        }
        return false;
    }

    public Slice type(Slice slice) {
        //We also check for ttl here
        if (!exists(slice)) {
            return Slice.create("none");
        }

        Slice value = get(slice, Slice.reserved());
        if (value == null) {
            return Slice.create("hash");
        }

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
