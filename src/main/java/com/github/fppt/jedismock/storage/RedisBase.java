package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.RedisClient;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class RedisBase {
    private final ExpiringKeyValueStorage keyValueStorage = new ExpiringKeyValueStorage();
    private final Map<Slice, Set<RedisClient>> subscribers = new ConcurrentHashMap<>();

    public RedisBase() {}

    public Set<Slice> keys(){
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

    public Slice getSlice(Slice key) {
        return keyValueStorage.getSlice(key);
    }

    public RMDataStructure getValue(Slice key) {
        return keyValueStorage.getValue(key);
    }

    public Map<Slice, Slice> getFieldsAndValues(Slice hash){
        return keyValueStorage.getFieldsAndValues(hash);
    }

    public Slice getSlice(Slice key1, Slice key2) {
        return keyValueStorage.getSlice(key1, key2);
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

    public void clear(){
        keyValueStorage.clear();
        subscribers.clear();
    }

    public void putSliceWithoutClearingTtl(Slice key, Slice value) {
        putSlice(key, value, null);
    }

    public void putSliceWithoutClearingTtl(Slice key1, Slice key2, Slice value) {
        putSlice(key1, key2, value, null);
    }

    public void putSlice(Slice key, Slice value){
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

    public boolean removeSubscriber(Slice channel, RedisClient client){
        if(subscribers.containsKey(channel)){
            subscribers.get(channel).remove(client);
            return true;
        }
        return false;
    }

    public Set<RedisClient> getSubscribers(Slice channel){
        if (subscribers.containsKey(channel)) {
            return subscribers.get(channel);
        }
        return Collections.emptySet();
    }

    public List<Slice> getSubscriptions(RedisClient client){
        List<Slice> subscriptions = new ArrayList<>();

        subscribers.forEach((channel, subscribers) -> {
            if(subscribers.contains(client)){
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
}
