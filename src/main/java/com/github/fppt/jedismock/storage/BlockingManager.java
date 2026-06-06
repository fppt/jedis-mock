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
 * on that key. A waiter is expected to register on entry and close its ticket on
 * exit, which is most naturally done with try-with-resources:
 * <pre>{@code
 * try (BlockingManager.Ticket ticket = blockingManager.register(keys)) {
 *     while (stillWaiting) {
 *         if (available(key) && ticket.isFirst(key)) {
 *             ...claim the element...
 *         }
 *         lock.wait(...);
 *     }
 * }
 * }</pre>
 * <p>
 * All methods (and {@link Ticket#close()}) must be called while holding the
 * shared data lock ({@link OperationExecutorState#lock()}), so no internal
 * synchronization is needed.
 */
public final class BlockingManager {
    private long nextTicket = 0L;
    private int blockedClients = 0;
    private final Map<Slice, NavigableSet<Long>> waitersByKey = new HashMap<>();

    /**
     * Register the caller as a FIFO waiter on each of the supplied keys.
     *
     * @return a {@link Ticket} that must be {@link Ticket#close() closed} once
     * the caller stops waiting (use try-with-resources). The ticket orders this
     * waiter after all earlier ones and keeps {@link #blockedClients()}
     * accurate.
     */
    public Ticket register(List<Slice> keys) {
        long id = nextTicket++;
        blockedClients++;
        for (Slice key : keys) {
            waitersByKey.computeIfAbsent(key, k -> new TreeSet<>()).add(id);
        }
        return new Ticket(id, keys);
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

    private boolean isFirst(long id, Slice key) {
        NavigableSet<Long> tickets = waitersByKey.get(key);
        return tickets == null || tickets.isEmpty() || tickets.first() == id;
    }

    private void unregister(long id, List<Slice> keys) {
        blockedClients--;
        for (Slice key : keys) {
            NavigableSet<Long> tickets = waitersByKey.get(key);
            if (tickets != null) {
                tickets.remove(id);
                if (tickets.isEmpty()) {
                    waitersByKey.remove(key);
                }
            }
        }
    }

    /**
     * A handle on a single blocked waiter. Holds its FIFO ticket and the keys it
     * is registered on, and unregisters them on {@link #close()}. Obtained from
     * {@link BlockingManager#register(List)} and intended to be used within a
     * try-with-resources block. Not thread-safe: like the rest of
     * {@link BlockingManager}, it must only be touched while holding the shared
     * data lock.
     */
    public final class Ticket implements AutoCloseable {
        private final long id;
        private final List<Slice> keys;
        private boolean closed = false;

        private Ticket(long id, List<Slice> keys) {
            this.id = id;
            this.keys = keys;
        }

        /**
         * @return whether this waiter is the oldest one blocked on {@code key}
         * and may therefore claim an element that has become available. Returns
         * {@code true} if no one is tracked for the key (defensive: never block
         * a client that somehow isn't registered).
         */
        public boolean isFirst(Slice key) {
            return BlockingManager.this.isFirst(id, key);
        }

        /**
         * Unregister this waiter from all of its keys. Idempotent, so an
         * explicit close followed by a try-with-resources close (or vice versa)
         * cannot corrupt {@link #blockedClients()}.
         */
        @Override
        public void close() {
            if (closed) {
                return;
            }
            closed = true;
            unregister(id, keys);
        }
    }
}
