package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.RMBitMap;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.RMHyperLogLog;
import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.streams.RMStream;
import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;

import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {
    private final Supplier<Clock> clockSupplier;
    private final RedisConfiguration configuration;
    private final SubscriptionRegistry subscriptionRegistry;
    private final int dbIndex;
    private final Map<Slice, Set<OperationExecutorState>> watchedKeys = new HashMap<>();
    private final Map<String, String> cachedLuaScripts = new HashMap<>();
    private final ExpiringKeyValueStorage keyValueStorage;

    public RedisBase(Supplier<Clock> clockSupplier) {
        this(clockSupplier, new RedisConfiguration(), new SubscriptionRegistry(), 0);
    }

    public RedisBase(Supplier<Clock> clockSupplier, RedisConfiguration configuration,
                     SubscriptionRegistry subscriptionRegistry, int dbIndex) {
        this.clockSupplier = Objects.requireNonNull(clockSupplier);
        this.configuration = Objects.requireNonNull(configuration);
        this.subscriptionRegistry = Objects.requireNonNull(subscriptionRegistry);
        this.dbIndex = dbIndex;
        this.keyValueStorage = new ExpiringKeyValueStorage(clockSupplier, key -> watchedKeys
                .getOrDefault(key, Collections.emptySet())
                .forEach(OperationExecutorState::watchedKeyIsAffected),
                this::notifyKeyspaceEvent);
    }

    /**
     * Publishes a keyspace notification for an event on a key of this
     * database, exactly like real Redis's {@code notifyKeyspaceEvent()}:
     * nothing is sent unless the event's class is enabled by
     * {@code notify-keyspace-events}, and the enabled channel families each
     * get their message — {@code __keyspace@<db>__:<key> -> <event>} and/or
     * {@code __keyevent@<db>__:<event> -> <key>}. Only call while holding
     * {@link OperationExecutorState#lock()} (all operations do).
     */
    public final void notifyKeyspaceEvent(KeyspaceEvent event, Slice key) {
        KeyspaceNotificationOptions options = configuration.getKeyspaceNotificationOptions();
        if (!options.isEnabled(event)) {
            return;
        }
        if (options.isEnabled(KeyspaceNotificationOptions.EventClass.KEYSPACE)) {
            subscriptionRegistry.publish(
                    channel("__keyspace@" + dbIndex + "__:", key.data()),
                    Slice.create(event.eventName()));
        }
        if (options.isEnabled(KeyspaceNotificationOptions.EventClass.KEYEVENT)) {
            subscriptionRegistry.publish(
                    channel("__keyevent@" + dbIndex + "__:", event.eventName().getBytes(StandardCharsets.UTF_8)),
                    key);
        }
    }

    /** Keys are binary-safe, so the channel name is built from bytes, not strings. */
    private static Slice channel(String prefix, byte[] suffix) {
        byte[] prefixBytes = prefix.getBytes(StandardCharsets.UTF_8);
        byte[] channel = new byte[prefixBytes.length + suffix.length];
        System.arraycopy(prefixBytes, 0, channel, 0, prefixBytes.length);
        System.arraycopy(suffix, 0, channel, prefixBytes.length, suffix.length);
        return Slice.create(channel);
    }

    public Clock getClock() {
        return clockSupplier.get();
    }

    public Set<Slice> keys() {
        Set<Slice> outdated = new HashSet<>();
        Set<Slice> result = new HashSet<>();
        for (Slice key : keyValueStorage.values().keySet()) {
            if (keyValueStorage.isKeyOutdated(key)) {
                outdated.add(key);
            } else {
                result.add(key);
            }
        }
        //Purge expired keys through the notifying delete (not a raw removal) so
        //that WATCHers of an expired key are flagged: a passive expiry counts as
        //a modification in real Redis, aborting a transaction that watched it.
        //Otherwise a DBSIZE/KEYS sweep would drop the key silently and a later
        //EXEC could no longer detect that the watched key had expired.
        for (Slice key : outdated) {
            keyValueStorage.deleteExpired(key);
        }
        return result;
    }

    public RMDataStructure getValue(Slice key) {
        return keyValueStorage.getValue(key);
    }

    private <T extends RMDataStructure> T getStructure(Slice key, Class<T> tClass) {
        RMDataStructure value = getValue(key);
        if (value == null) {
            return null;
        }
        if (tClass.isInstance(value)) {
            return (T) value;
        }
        value.raiseTypeCastException();
        return null;
    }

    public RMStream getStream(Slice key) {
        return getStructure(key, RMStream.class);
    }

    public RMSet getSet(Slice key) {
        return getStructure(key, RMSet.class);
    }

    public RMZSet getZSet(Slice key) {
        return getStructure(key, RMZSet.class);
    }

    public RMList getList(Slice key) {
        return getStructure(key, RMList.class);
    }

    public RMHash getHash(Slice key) {
        return getStructure(key, RMHash.class);
    }

    public RMHyperLogLog getHLL(Slice key) {
        return getStructure(key, RMHyperLogLog.class);
    }

    public RMString getRMString(Slice key) {
        return getStructure(key, RMString.class);
    }

    public RMBitMap getBitMap(Slice key) {
        RMDataStructure value = getValue(key);
        if (value == null) {
            return null;
        }
        if (value instanceof RMBitMap) {
            return (RMBitMap) value;
        }
        if (value instanceof RMString) {
            return new RMBitMap(((RMString) value).getStoredData());
        }
        value.raiseTypeCastException();
        return null;
    }

    public Slice getSlice(Slice key) {
        RMDataStructure value = getValue(key);
        if (value == null) {
            return null;
        }
        return value.getAsSlice();
    }

    public Slice getSlice(Slice key1, Slice key2) {
        RMHash hashTable = getHash(key1);
        if (hashTable == null) {
            return null;
        }
        return hashTable.get(key2);
    }

    public Map<Slice, Slice> getFieldsAndValuesReadOnly(Slice hash) {
        RMHash hashTable = getHash(hash);
        if (hashTable == null) {
            return Collections.emptyMap();
        }
        return hashTable.getStoredDataReadOnly();
    }

    public Long getTTL(Slice key) {
        return keyValueStorage.getTTL(key);
    }

    public long setTTL(Slice key, long ttl) {
        return keyValueStorage.setTTL(key, ttl);
    }

    public long setDeadline(Slice key, long deadline) {
        return keyValueStorage.setDeadline(key, deadline);
    }

    public Long getDeadline(Slice key) {
        return keyValueStorage.getDeadline(key);
    }

    public void clear() {
        keyValueStorage.clear();
    }

    public void putSlice(Slice key, Slice value, Long ttl) {
        keyValueStorage.put(key, value, ttl);
    }

    public void putSlice(Slice key1, Slice key2, Slice value, Long ttl) {
        keyValueStorage.put(key1, key2, value, ttl);
    }

    public void putValueWithoutClearingTtl(Slice key, RMDataStructure value) {
        putValue(key, value, null);
    }

    public void putValue(Slice key, RMDataStructure value, Long ttl) {
        keyValueStorage.put(key, value, ttl);
    }

    public void putValue(Slice key, RMDataStructure value) {
        keyValueStorage.put(key, value, -1L);
    }

    public void deleteValue(Slice key) {
        keyValueStorage.delete(key);
    }

    public void deleteValue(Slice key1, Slice key2) {
        keyValueStorage.delete(key1, key2);
    }

    public boolean exists(Slice slice) {
        return keyValueStorage.exists(slice);
    }

    public Slice type(Slice slice) {
        return keyValueStorage.type(slice);
    }

    public void watch(OperationExecutorState state, Slice key) {
        watchedKeys.computeIfAbsent(key, k -> new HashSet<>()).add(state);
    }

    public void unwatchSingleKey(OperationExecutorState state, Slice key) {
        Set<OperationExecutorState> states = watchedKeys.get(key);
        if (states != null) {
            states.remove(state);
            if (states.isEmpty()) {
                watchedKeys.remove(key);
            }
        }
    }

    public void markKeyModified(Slice key) {
        watchedKeys.getOrDefault(key, new HashSet<>()).forEach(OperationExecutorState::watchedKeyIsAffected);
    }

    public long getProtoMaxBulkLen() {
        return configuration.getProtoMaxBulkLen();
    }

    public String getCachedLuaScript(String sha1) {
        return cachedLuaScripts.get(sha1.toLowerCase());
    }

    public boolean cachedLuaScriptExists(String sha1) {
        return cachedLuaScripts.containsKey(sha1.toLowerCase());
    }

    public void flushCachedLuaScrips() {
        cachedLuaScripts.clear();
    }

    public String addCachedLuaScript(String sha1, String script) {
        return cachedLuaScripts.put(sha1, script);
    }
}
