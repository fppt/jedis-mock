package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.RMSortedSet;
import com.github.fppt.jedismock.server.RedisClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {
    private final ExpiringKeyValueStorage keyValueStorage =
            new ExpiringKeyValueStorage(this::notifyClientsAboutKeyAffection);
    private final Map<Slice, Set<RedisClient>> subscribers = new HashMap<>();
    private final HashMap<Object, Set<OperationExecutorState>> watchedKeys = new HashMap<>();

    public Set<Slice> keys() {
        Set<Slice> slices = keyValueStorage.values().keySet();
        Set<Slice> result = new HashSet<>();
        for (Slice key: slices){
            Long deadline = keyValueStorage.ttls().get(key);
            if (deadline != null && deadline != -1 && deadline <= System.currentTimeMillis()) {
                keyValueStorage.delete(key);
            } else {
                result.add(key);
            }
        }
        return result;
    }

    public RMDataStructure getValue(Slice key) {
        return keyValueStorage.getValue(key);
    }

    public RMSet getSet(Slice key) {
        RMDataStructure value = getValue(key);

        if (value == null) {
            return null;
        }

        if(!(value instanceof RMSet)) {
            value.raiseTypeCastException();
        }

        return (RMSet) value;
    }

    public RMHMap getMap(Slice key) {
        RMDataStructure value = getValue(key);

        if (value == null) {
            return null;
        }

        if(!(value instanceof RMHMap)) {
            value.raiseTypeCastException();
        }

        return (RMHMap) value;
    }


    public RMList getList(Slice key) {
        RMDataStructure value = getValue(key);

        if (value == null) {
            return null;
        }

        if(!(value instanceof RMList)) {
            value.raiseTypeCastException();
        }

        return (RMList) value;
    }

    public Slice getSlice(Slice key) {
        RMDataStructure value = getValue(key);

        if (value == null) {
            return null;
        }

        if(!(value instanceof Slice)) {
            value.raiseTypeCastException();
        }

        return (Slice) value;
    }

    public Slice getSlice(Slice key1, Slice key2) {
        RMSortedSet value = getSortedSet(key1);

        if (value == null) {
            return null;
        }

        Map<Slice, Slice> innerMap = value.getStoredData();
        if(innerMap == null) {
            return null;
        }

        return innerMap.get(key2);
    }


    private RMSortedSet getSortedSet(Slice key) {
        RMDataStructure value = getValue(key);

        if (value == null) {
            return null;
        }

        if(!(value instanceof RMSortedSet)) {
            value.raiseTypeCastException();
        }

        return (RMSortedSet) value;
    }

    public Map<Slice, Slice> getFieldsAndValues(Slice hash){
        RMSortedSet sortedSet = getSortedSet(hash);
        if(sortedSet == null) {
            return null;
        }
        return sortedSet.getStoredData();
    }

    public Long getTTL(Slice key) {
        return keyValueStorage.getTTL(key);
    }

    public final void notifyClientsAboutKeyAffection(Slice key) {
        for (OperationExecutorState state : watchedKeys.getOrDefault(key, Collections.emptySet())) {
            state.watchedKeyIsAffected();
        }
    }

    public long setTTL(Slice key, long ttl) {
        return keyValueStorage.setTTL(key, ttl);
    }

    public long setDeadline(Slice key, long deadline) {
        return keyValueStorage.setDeadline(key, deadline);
    }

    public void clear() {
        keyValueStorage.clear();
        subscribers.clear();
    }

    public void putSliceWithoutClearingTtl(Slice key, Slice value) {
        putSlice(key, value, null);
    }

    public void putSliceWithoutClearingTtl(Slice key1, Slice key2, Slice value) {
        putSlice(key1, key2, value, null);
    }

    public void putSlice(Slice key, Slice value) {
        putSlice(key, value, -1L);
    }

    public void putSlice(Slice key, Slice value, Long ttl) {
        keyValueStorage.put(key, value, ttl);
    }

    public void putSlice(Slice key1, Slice key2, Slice value, Long ttl) {
        keyValueStorage.put(key1, key2, value, ttl);
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

    public void addSubscriber(Slice channel, RedisClient client){
        Set<RedisClient> newClient = new HashSet<>();
        newClient.add(client);
        subscribers.merge(channel, newClient, (currentSubscribers, newSubscribers) -> {
            currentSubscribers.addAll(newSubscribers);
            return currentSubscribers;
        });
    }

    public boolean removeSubscriber(Slice channel, RedisClient client) {
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
        if (subscribers.containsKey(channel)) {
            return subscribers.get(channel);
        }
        return Collections.emptySet();
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
        }
    }
}
