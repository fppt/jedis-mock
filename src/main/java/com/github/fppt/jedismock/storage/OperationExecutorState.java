package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.RedisClient;

import java.time.Clock;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

public class OperationExecutorState {
    public enum TransactionState {NORMAL, MULTI, ERRORED}

    private final RedisClient owner;
    private final Map<Integer, RedisBase> redisBases;
    private TransactionState transactionState = TransactionState.NORMAL;
    private final List<RedisOperation> tx = new ArrayList<>();
    private final Set<Slice> watchedKeys = new HashSet<>();
    private boolean watchedKeysAffected = false;
    private int selectedRedisBase = 0;
    private String clientName;

    public OperationExecutorState(RedisClient owner, Map<Integer, RedisBase> redisBases) {
        this.owner = owner;
        this.redisBases = redisBases;
    }

    public RedisBase base() {
        return base(selectedRedisBase);
    }

    public RedisBase base(int baseIndex) {
        return redisBases.computeIfAbsent(baseIndex, key -> new RedisBase(this::getClock));
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
        this.transactionState = isTransactionModeOn ? TransactionState.MULTI : TransactionState.NORMAL;
    }

    public boolean isTransactionModeOn() {
        return transactionState != TransactionState.NORMAL;
    }

    public void errorTransaction() {
        if (isTransactionModeOn()) {
            transactionState = TransactionState.ERRORED;
        }
    }

    public TransactionState getTransactionState() {
        return transactionState;
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

    public Clock getClock() {
        return owner.getClock();
    }

    public String getServerHost() {
        return owner.getServerHost();
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    public String getClientName() {
        return clientName;
    }
}
