package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.server.Response;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Server-wide registry of channel and pattern subscriptions.
 * <p>
 * Pub/Sub in Redis is completely independent of the key space: messages
 * cross database boundaries, and clearing a database (FLUSHDB/FLUSHALL)
 * does not affect subscriptions. This registry is therefore owned by the
 * server (like {@link RedisConfiguration}) rather than by a per-database
 * {@link RedisBase}, and shared by every client of that server.
 * <p>
 * Thread safety is provided by this class itself: every method below is
 * synchronized on the registry, so callers need no external lock. In
 * particular it is deliberately <em>not</em> guarded by the shared data lock
 * ({@link OperationExecutorState#lock()}), because a disconnecting client has
 * to deregister itself and must never wait behind a long-running Lua script
 * that holds that lock.
 * <p>
 * <b>Lock ordering.</b> Commands acquire the data lock first and this monitor
 * second (a pub/sub command runs inside {@code MockExecutor}'s synchronized
 * block and calls the methods below); nothing acquires them the other way
 * round, so the two cannot deadlock. To keep that true, the synchronized
 * methods here must never call out to anything that can take the data lock,
 * {@code wait()} on it, or write to a socket — which is why {@link #publish}
 * is not synchronized: it snapshots through the copying accessors and only
 * then performs I/O.
 */
public class SubscriptionRegistry {
    private final Map<Slice, Set<RedisClient>> subscribers = new HashMap<>();
    private final Map<Slice, Set<RedisClient>> psubscribers = new HashMap<>();

    /**
     * @return whether this added a new subscription (false for a duplicate),
     * so that callers can maintain the running count for the per-argument
     * acknowledgements without rescanning the registry.
     */
    public synchronized boolean addSubscriber(Slice channel, RedisClient client) {
        return subscribers.computeIfAbsent(channel, c -> new HashSet<>()).add(client);
    }

    /**
     * @return whether this added a new subscription (false for a duplicate),
     * so that callers can maintain the running count for the per-argument
     * acknowledgements without rescanning the registry.
     */
    public synchronized boolean subscribeByPattern(Slice pattern, RedisClient client) {
        return psubscribers.computeIfAbsent(pattern, p -> new HashSet<>()).add(client);
    }

    /**
     * @return whether the client was actually subscribed to the channel.
     */
    public synchronized boolean removeSubscriber(Slice channel, RedisClient client) {
        return removeSubscriber(channel, client, subscribers);
    }

    /**
     * @return whether the client was actually subscribed to the pattern.
     */
    public synchronized boolean removePSubscriber(Slice channel, RedisClient client) {
        return removeSubscriber(channel, client, psubscribers);
    }

    private static boolean removeSubscriber(Slice channel, RedisClient client, Map<Slice, Set<RedisClient>> subscribers) {
        Set<RedisClient> redisClients = subscribers.get(channel);
        if (redisClients == null) {
            return false;
        }
        boolean removed = redisClients.remove(client);
        if (redisClients.isEmpty()) {
            subscribers.remove(channel);
        }
        return removed;
    }

    public synchronized Set<RedisClient> getSubscribers(Slice channel) {
        Set<RedisClient> subs = new HashSet<>();
        if (subscribers.containsKey(channel)) {
            subs.addAll(subscribers.get(channel));
        }
        return subs;
    }

    public synchronized int getSubscribersCount(Slice channel) {
        return subscribers.getOrDefault(channel, Collections.emptySet()).size();
    }

    /**
     * @return the total number of channel and pattern subscriptions held by
     * the client — the count Redis reports in every (p)subscribe and
     * (p)unsubscribe acknowledgement.
     */
    public synchronized int getSubscriptionsCount(RedisClient client) {
        return getSubscriptions(client).size() + getPSubscriptions(client).size();
    }

    public synchronized Map<Slice, Set<RedisClient>> getPsubscribers(Slice channel) {
        Map<Slice, Set<RedisClient>> matchingPatterns = new HashMap<>();
        String channelStr = channel.toString();
        for (Map.Entry<Slice, Set<RedisClient>> patternSubscribedClients : psubscribers.entrySet()) {
            Slice jedisPattern = patternSubscribedClients.getKey();
            String regexpPattern = getRegexpFromPattern(jedisPattern);
            if (!channelStr.matches(regexpPattern)) {
                continue;
            }
            //Defensive copy: never leak the internal, mutable subscriber sets
            matchingPatterns.put(jedisPattern, new HashSet<>(patternSubscribedClients.getValue()));
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

    public synchronized int getNumpat() {
        return psubscribers.size();
    }

    public synchronized Set<Slice> getChannels() {
        //Defensive copy: keySet() is a live view backed by the internal map
        return new HashSet<>(subscribers.keySet());
    }

    public synchronized List<Slice> getSubscriptions(RedisClient client) {
        return getSubscriptions(client, subscribers);
    }

    public synchronized List<Slice> getPSubscriptions(RedisClient client) {
        return getSubscriptions(client, psubscribers);
    }

    /**
     * Delivers a message to every channel subscriber and every client whose
     * pattern matches the channel — the delivery logic of {@code PUBLISH},
     * shared with server-initiated messages such as keyspace notifications.
     * Only call while holding {@link OperationExecutorState#lock()}: delivery
     * under the lock is what preserves Redis's ordering guarantee between a
     * subscribe acknowledgement and the first message (see issue #768).
     * <p>
     * Deliberately not synchronized on this registry: it reads the subscriber
     * sets through the accessors above (which copy them) and then writes to
     * sockets, so a slow client cannot block another client's disconnect.
     *
     * @return the number of clients the message was delivered to
     */
    public int publish(Slice channel, Slice message) {
        Set<RedisClient> channelSubscribers = getSubscribers(channel);
        for (RedisClient subscriber : channelSubscribers) {
            subscriber.sendResponse(Response.publishedMessage(channel, message), "contacting subscriber");
        }
        int deliveries = channelSubscribers.size();
        for (Map.Entry<Slice, Set<RedisClient>> patternSubscribers : getPsubscribers(channel).entrySet()) {
            Slice pattern = patternSubscribers.getKey();
            for (RedisClient psubscriber : patternSubscribers.getValue()) {
                psubscriber.sendResponse(Response.publishedPMessage(pattern, channel, message), "contacting subscriber");
            }
            deliveries += patternSubscribers.getValue().size();
        }
        return deliveries;
    }

    /**
     * Drops all channel and pattern subscriptions of a client. Called when
     * the client disconnects: like in real Redis, a dead connection must not
     * linger in the pub/sub registries.
     */
    public synchronized void removeClient(RedisClient client) {
        removeClient(client, subscribers);
        removeClient(client, psubscribers);
    }

    private static void removeClient(RedisClient client, Map<Slice, Set<RedisClient>> subscribers) {
        subscribers.values().forEach(clients -> clients.remove(client));
        subscribers.values().removeIf(Set::isEmpty);
    }

    private static List<Slice> getSubscriptions(RedisClient client, Map<Slice, Set<RedisClient>> subscribers) {
        List<Slice> subscriptions = new ArrayList<>();

        subscribers.forEach((channel, clients) -> {
            if (clients.contains(client)) {
                subscriptions.add(channel);
            }
        });

        return subscriptions;
    }
}
