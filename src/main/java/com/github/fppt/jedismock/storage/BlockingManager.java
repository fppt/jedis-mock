package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableSet;
import java.util.TreeSet;

/**
 * Tracks clients blocked on keys (BLPOP, BRPOP, BRPOPLPUSH, …) so they can be
 * served in FIFO order, the way real Redis does.
 * <p>
 * Without this, a push wakes every blocked client via {@code notifyAll()} and
 * whichever thread wins the (unfair) intrinsic-monitor race consumes the
 * element. That breaks ordering guarantees and can hang a client forever: e.g.
 * if a later waiter steals the only element, the older waiter re-checks, finds
 * nothing, and blocks again with no further notification.
 * <p>
 * Each blocked client takes a monotonically increasing ticket and registers it
 * against every key it waits on. A woken client may only claim an element from a
 * key if it holds the lowest (oldest) ticket among the clients currently blocked
 * on that key.
 * <p>
 * All methods must be called while holding the shared data lock
 * ({@link OperationExecutorState#lock()}), so no internal synchronization is
 * needed.
 */
public final class BlockingManager {
    private long nextTicket = 0L;
    private int blockedClients = 0;
    private final Map<Slice, NavigableSet<Long>> waitersByKey = new HashMap<>();

    /**
     * @return a fresh ticket, ordering this waiter after all earlier ones.
     */
    public long nextTicket() {
        return nextTicket++;
    }

    /**
     * Record that the given ticket is blocked on each of the supplied keys.
     * Must be paired with exactly one {@link #unregister} call per blocked
     * client so that {@link #blockedClients()} stays accurate.
     */
    public void register(long ticket, List<Slice> keys) {
        blockedClients++;
        for (Slice key : keys) {
            waitersByKey.computeIfAbsent(key, k -> new TreeSet<>()).add(ticket);
        }
    }

    /**
     * Remove the given ticket from all of the supplied keys.
     */
    public void unregister(long ticket, List<Slice> keys) {
        blockedClients--;
        for (Slice key : keys) {
            NavigableSet<Long> tickets = waitersByKey.get(key);
            if (tickets != null) {
                tickets.remove(ticket);
                if (tickets.isEmpty()) {
                    waitersByKey.remove(key);
                }
            }
        }
    }

    /**
     * @return the number of clients currently blocked on a blocking command.
     * Mirrors the {@code blocked_clients} field of Redis {@code INFO clients},
     * which test suites poll (via {@code wait_for_blocked_client}) to
     * synchronize before unblocking.
     */
    public int blockedClients() {
        return blockedClients;
    }

    /**
     * @return whether the given ticket is the oldest waiter on {@code key} and
     * may therefore claim an element that has become available. Returns
     * {@code true} if no one is tracked for the key (defensive: never block a
     * client that somehow isn't registered).
     */
    public boolean isFirst(long ticket, Slice key) {
        NavigableSet<Long> tickets = waitersByKey.get(key);
        return tickets == null || tickets.isEmpty() || tickets.first() == ticket;
    }
}
