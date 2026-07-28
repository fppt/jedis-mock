package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;

import java.util.ArrayList;
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
 * {@link RedisBase}. Shared by every client of the same server; only
 * mutate it while holding {@link OperationExecutorState#lock()}.
 */
public class SubscriptionRegistry {
    private final Map<Slice, Set<RedisClient>> subscribers = new HashMap<>();
    private final Map<Slice, Set<RedisClient>> psubscribers = new HashMap<>();

    public void addSubscriber(Slice channel, RedisClient client) {
        subscribers.computeIfAbsent(channel, c -> new HashSet<>()).add(client);
    }

    public void subscribeByPattern(Slice pattern, RedisClient client) {
        psubscribers.computeIfAbsent(pattern, p -> new HashSet<>()).add(client);
    }

    public boolean removeSubscriber(Slice channel, RedisClient client) {
        return removeSubscriber(channel, client, subscribers);
    }

    public boolean removePSubscriber(Slice channel, RedisClient client) {
        return removeSubscriber(channel, client, psubscribers);
    }

    private static boolean removeSubscriber(Slice channel, RedisClient client, Map<Slice, Set<RedisClient>> subscribers) {
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
        Set<RedisClient> subs = new HashSet<>();
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
        return getSubscriptions(client, subscribers);
    }

    public List<Slice> getPSubscriptions(RedisClient client) {
        return getSubscriptions(client, psubscribers);
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
