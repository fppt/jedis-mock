package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.server.RedisClient;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicBoolean;

public class OperationExecutorState {
    private final RedisClient owner;
    private final Map<Integer, RedisBase> redisBases;
    private final AtomicBoolean isTransactionModeOn = new AtomicBoolean(false);
    private final List<RedisOperation> tx = new ArrayList<>();
    public final Set<Slice> watchedKeys = new HashSet<>();
    private boolean watchedKeysAffected = false;
    private int selectedRedisBase = 0;
    private String clientName;

    public OperationExecutorState(RedisClient owner, Map<Integer, RedisBase> redisBases) {
        this.owner = owner;
        this.redisBases = redisBases;
    }

    public RedisBase base() {
        return redisBases.computeIfAbsent(selectedRedisBase, key -> new RedisBase());
    }

    public RedisClient owner() {
        return owner;
    }

    public List<RedisOperation> tx() {
        return tx;
    }

    public void changeActiveRedisBase(int selectedRedisBase) {
        this.selectedRedisBase = selectedRedisBase;
    }

    public void transactionMode(boolean isTransactionModeOn) {
        this.isTransactionModeOn.set(isTransactionModeOn);
    }

    public boolean isTransactionModeOn() {
        return isTransactionModeOn.get();
    }

    public void newTransaction() {
        if (isTransactionModeOn.get()) {
            throw new IllegalStateException("Redis mock does not support more than one transaction");
        }
        transactionMode(true);
    }

    public void clearAll() {
        for (RedisBase redisBase : redisBases.values()) {
            redisBase.clear();
        }
    }

    public Object lock() {
        return redisBases;
    }

    public void checkWatchedKeysNotExpired() {
        for (Slice key : watchedKeys) {
            base().exists(key);
        }
    }

    public boolean isValid() {
        return !watchedKeysAffected;
    }

    public void watchedKeyIsAffected() {
        watchedKeysAffected = true;
    }

    public void watch(List<Slice> keys) {
        RedisBase redisBase = base();
        for (Slice key : keys) {
            watchedKeys.add(key);
            redisBase.watch(this, key);
        }
    }

    public void unwatch() {
        RedisBase redisBase = base();
        for (Slice key : watchedKeys) {
            redisBase.unwatchSingleKey(this, key);
        }
        watchedKeysAffected = false;
    }

    public int getSelected() {
        return selectedRedisBase;
    }

    public int getPort() {
        return owner.getPort();
    }

    public String getHost() {
        return owner.getAddress();
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getClientName() {
        return clientName;
    }
}
