package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.Utils;
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
import com.github.fppt.jedismock.RedisClient;

import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {
    private final Supplier<Clock> clockSupplier;
    private final Map<Slice, Set<RedisClient>> subscribers = new HashMap<>();
    private final Map<Slice, Set<RedisClient>> psubscribers = new HashMap<>();
    private final Map<Slice, Set<OperationExecutorState>> watchedKeys = new HashMap<>();
    private final Map<String, String> cachedLuaScripts = new HashMap<>();
    private final ExpiringKeyValueStorage keyValueStorage;

    public RedisBase(Supplier<Clock> clockSupplier) {
        this.clockSupplier = Objects.requireNonNull(clockSupplier);
        this.keyValueStorage = new ExpiringKeyValueStorage(clockSupplier, key -> watchedKeys
                .getOrDefault(key, Collections.emptySet())
                .forEach(OperationExecutorState::watchedKeyIsAffected));
    }

    public Clock getClock() {
        return clockSupplier.get();
    }

    public Set<Slice> keys() {
        Iterator<Slice> slices = keyValueStorage.values().keySet().iterator();
        Set<Slice> result = new HashSet<>();
        while (slices.hasNext()) {
            Slice key = slices.next();
            if (keyValueStorage.isKeyOutdated(key)) {
                slices.remove();
            } else {
                result.add(key);
            }
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
        RMHash value = getHash(key1);

        if (value == null) {
            return null;
        }

        Map<Slice, Slice> innerMap = value.getStoredData();
        if (innerMap == null) {
            return null;
        }

        return innerMap.get(key2);
    }

    public Map<Slice, Slice> getFieldsAndValues(Slice hash) {
        RMHash hashTable = getHash(hash);
        if (hashTable == null) {
            return Collections.emptyMap();
        }
        return hashTable.getStoredData();
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
        return keyValueStorage.ttls().get(key);
    }

    public void clear() {
        keyValueStorage.clear();
        subscribers.clear();
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

    public void addSubscriber(Slice channel, RedisClient client) {
        Set<RedisClient> newClient = new HashSet<>();
        newClient.add(client);
        subscribers.merge(channel, newClient, (currentSubscribers, newSubscribers) -> {
            currentSubscribers.addAll(newSubscribers);
            return currentSubscribers;
        });
    }

    public void subscribeByPattern(Slice pattern, RedisClient client) {
        Set<RedisClient> newClient = new HashSet<>();
        newClient.add(client);
        psubscribers.merge(pattern, newClient, (currentSubscribers, newSubscribers) -> {
            currentSubscribers.addAll(newSubscribers);
            return currentSubscribers;
        });
    }

    public boolean removeSubscriber(Slice channel, RedisClient client) {
        return removeSubscriber(channel, client, subscribers);
    }

    public boolean removePSubscriber(Slice channel, RedisClient client) {
        return removeSubscriber(channel, client, psubscribers);
    }

    private boolean removeSubscriber(Slice channel, RedisClient client, Map<Slice, Set<RedisClient>> subscribers) {
        if (subscribers.containsKey(channel)) {
            Set<RedisClient> redisClients = subscribers.get(channel);
            redisClients.remove(client);
            if (redisClients.isEmpty()) {
                subscribers.remove(channel);
            }
            return true;
        }
        return false;
    }

    public Set<RedisClient> getSubscribers(Slice channel) {
        Set<RedisClient> subs = new HashSet<>(Collections.emptySet());
        if (subscribers.containsKey(channel)) {
            subs.addAll(subscribers.get(channel));
        }
        return subs;
    }

    public Map<Slice, Set<RedisClient>> getPsubscribers(Slice channel) {
        Map<Slice, Set<RedisClient>> matchingPatterns = new HashMap<>();
        String channelStr = channel.toString();
        for (Map.Entry<Slice, Set<RedisClient>> patternSubscribedClients : psubscribers.entrySet()) {
            Slice jedisPattern = patternSubscribedClients.getKey();
            String regexpPattern = getRegexpFromPattern(jedisPattern);
            if (!channelStr.matches(regexpPattern)) {
                continue;
            }
            matchingPatterns.put(jedisPattern, patternSubscribedClients.getValue());
        }
        return matchingPatterns;
    }

    private static String getRegexpFromPattern(Slice pattern) {
        String patternStr = pattern.toString();
        if (patternStr.isEmpty()) {
            return ".*";
        }
        return Utils.createRegexFromGlob(patternStr);
    }

    public int getNumpat() {
        return psubscribers.size();
    }

    public Set<Slice> getChannels() {
        return subscribers.keySet();
    }

    public List<Slice> getSubscriptions(RedisClient client) {
        List<Slice> subscriptions = new ArrayList<>();

        subscribers.forEach((channel, subscribers) -> {
            if (subscribers.contains(client)) {
                subscriptions.add(channel);
            }
        });

        return subscriptions;
    }

    public List<Slice> getPSubscriptions(RedisClient client) {
        List<Slice> subscriptions = new ArrayList<>();

        psubscribers.forEach((channel, subscribers) -> {
            if (subscribers.contains(client)) {
                subscriptions.add(channel);
            }
        });

        return subscriptions;
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
