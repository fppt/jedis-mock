package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;

import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Supplier;

public abstract class ExpiringStorage {
    protected final Consumer<Slice> keyChangeNotifier;

    private final Supplier<Clock> clockSupplier;
    private final Map<Slice, Long> ttls = new HashMap<>();

    public ExpiringStorage(Supplier<Clock> clockSupplier, Consumer<Slice> keyChangeNotifier) {
        this.clockSupplier = Objects.requireNonNull(clockSupplier);
        this.keyChangeNotifier = Objects.requireNonNull(keyChangeNotifier);
    }

    protected void clear() {
        ttls.clear();
    }

    protected void delete(Slice key) {
        ttls.remove(key);
    }

    protected abstract boolean keyExists(Slice key);

    protected boolean isKeyOutdated(Slice key) {
        Long deadline = ttls.get(key);
        return deadline != null && deadline != -1 && deadline <= getMillis();
    }

    public Long getTTL(Slice key) {
        Objects.requireNonNull(key);
        Long deadline = ttls.get(key);
        if (deadline == null) {
            return null;
        }
        if (deadline == -1) {
            return deadline;
        }
        long now = getMillis();
        if (now < deadline) {
            return deadline - now;
        }
        delete(key);
        return null;
    }

    public Long getDeadline(Slice key) {
        return ttls.get(key);
    }

    public long setTTL(Slice key, long ttl) {
        keyChangeNotifier.accept(key);
        return setDeadline(key, ttl + getMillis());
    }

    protected final long getMillis() {
        return clockSupplier.get().millis();
    }

    protected void configureTTL(Slice key, Long ttl) {
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
        if (keyExists(key)) {
            Long oldValue = ttls.put(key, deadline);
            //It is considered to be an unsuccessful operation if we
            //reset deadline for the key which does not have one
            return (deadline < 0 && (oldValue == null || oldValue < 0)) ?
                    0L : 1L;
        }
        return 0L;
    }

    public Supplier<Clock> getClockSupplier() {
        return clockSupplier;
    }
}
