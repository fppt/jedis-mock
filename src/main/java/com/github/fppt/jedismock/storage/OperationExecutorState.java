package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.server.Response;

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
    private final BlockingManager blockingManager;
    private final ScriptingManager scriptingManager;
    private final RedisConfiguration configuration;
    private final SubscriptionRegistry subscriptionRegistry;
    private TransactionState transactionState = TransactionState.NORMAL;
    private final List<RedisOperation> tx = new ArrayList<>();
    private final Set<Slice> watchedKeys = new HashSet<>();
    private boolean watchedKeysAffected = false;
    private int selectedRedisBase = 0;
    private String clientName;
    private boolean repliesDisabled = false;
    private int repliesToSkip = 0;

    public OperationExecutorState(RedisClient owner, Map<Integer, RedisBase> redisBases) {
        this(owner, redisBases, new BlockingManager(), new ScriptingManager(), new RedisConfiguration(),
                new SubscriptionRegistry());
    }

    public OperationExecutorState(RedisClient owner, Map<Integer, RedisBase> redisBases,
                                  BlockingManager blockingManager,
                                  ScriptingManager scriptingManager,
                                  RedisConfiguration configuration,
                                  SubscriptionRegistry subscriptionRegistry) {
        this.owner = owner;
        this.redisBases = redisBases;
        this.blockingManager = blockingManager;
        this.scriptingManager = scriptingManager;
        this.configuration = configuration;
        this.subscriptionRegistry = subscriptionRegistry;
    }

    public RedisBase base() {
        return base(selectedRedisBase);
    }

    public RedisBase base(int baseIndex) {
        return redisBases.computeIfAbsent(baseIndex,
                key -> new RedisBase(this::getClock, configuration, subscriptionRegistry, key));
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

    /**
     * @return the server-wide registry used to serve blocked clients in FIFO
     * order. Shared by every client of the same server; only mutate it while
     * holding {@link #lock()}.
     */
    public BlockingManager blockingManager() {
        return blockingManager;
    }

    /**
     * @return the server-wide registry coordinating Lua script execution (used
     * by {@code SCRIPT KILL}). Shared by every client of the same server.
     */
    public ScriptingManager scriptingManager() {
        return scriptingManager;
    }

    /**
     * @return the server-wide thin {@code CONFIG} namespace (a plain key→value
     * store for parameters that don't affect mock behaviour). Shared by every
     * client of the same server.
     */
    public RedisConfiguration configuration() {
        return configuration;
    }

    /**
     * @return the server-wide registry of channel and pattern subscriptions.
     * Pub/Sub is independent of the key space, so the registry is shared by
     * every client and every database of the same server. It synchronizes
     * itself and is deliberately not covered by {@link #lock()}, so that a
     * disconnecting client never waits behind a running script.
     */
    public SubscriptionRegistry subscriptionRegistry() {
        return subscriptionRegistry;
    }

    /**
     * @return whether the client owning this state is still connected. Used by
     * blocking operations to cancel themselves when their client disconnects,
     * so they don't consume data intended for later clients.
     */
    public boolean isClientConnected() {
        return owner.isConnected();
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

    /** {@code CLIENT REPLY ON}: resume replying (and always reply +OK itself). */
    public void replyOn() {
        repliesDisabled = false;
        repliesToSkip = 0;
    }

    /** {@code CLIENT REPLY OFF}: suppress all command replies until ON. */
    public void replyOff() {
        repliesDisabled = true;
    }

    /**
     * {@code CLIENT REPLY SKIP}: suppress the reply to this command and to
     * the next one. A no-op while replies are OFF, matching real Redis.
     */
    public void replySkip() {
        if (!repliesDisabled) {
            repliesToSkip = 2;
        }
    }

    /**
     * Applies the {@code CLIENT REPLY} mode to a command reply. Called only on
     * the socket command-reply path — never for pub/sub pushes (subscribe
     * acknowledgements, published messages), which real Redis delivers even
     * while replies are silenced.
     */
    public Slice applyReplyMode(Slice response) {
        if (repliesToSkip > 0) {
            repliesToSkip--;
            return Response.SKIP;
        }
        if (repliesDisabled) {
            return Response.SKIP;
        }
        return response;
    }
}
