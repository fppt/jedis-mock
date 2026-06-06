package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.datastructures.Slice;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.github.fppt.jedismock.datastructures.Slice.create;
import static org.assertj.core.api.Assertions.assertThat;

/**
 * Unit tests for {@link BlockingManager}. They drive the manager directly (no
 * server/lock), which is safe because all access in production already happens
 * under a single shared lock, so single-threaded calls here faithfully mirror
 * the real serialized access pattern.
 */
class BlockingManagerTest {

    private final BlockingManager manager = new BlockingManager();

    private static List<Slice> keys(String... names) {
        List<Slice> result = new ArrayList<>();
        for (String name : names) {
            result.add(create(name));
        }
        return result;
    }

    @Test
    void blockedClientsStartsAtZero() {
        assertThat(manager.blockedClients()).isZero();
    }

    @Test
    void registerIncrementsAndCloseDecrementsBlockedClients() {
        BlockingManager.Ticket ticket = manager.register(keys("a"));
        assertThat(manager.blockedClients()).isEqualTo(1);

        ticket.close();
        assertThat(manager.blockedClients()).isZero();
    }

    @Test
    void blockedClientsCountsEveryRegisteredWaiter() {
        BlockingManager.Ticket t1 = manager.register(keys("a"));
        BlockingManager.Ticket t2 = manager.register(keys("a", "b"));
        BlockingManager.Ticket t3 = manager.register(keys("c"));

        assertThat(manager.blockedClients()).isEqualTo(3);

        t2.close();
        assertThat(manager.blockedClients()).isEqualTo(2);

        t1.close();
        t3.close();
        assertThat(manager.blockedClients()).isZero();
    }

    @Test
    void oldestWaiterIsServedFirst() {
        Slice key = create("a");
        BlockingManager.Ticket first = manager.register(Collections.singletonList(key));
        BlockingManager.Ticket second = manager.register(Collections.singletonList(key));

        assertThat(first.isFirst(key)).isTrue();
        assertThat(second.isFirst(key)).isFalse();
    }

    @Test
    void closingOldestPromotesNextInLine() {
        Slice key = create("a");
        BlockingManager.Ticket first = manager.register(Collections.singletonList(key));
        BlockingManager.Ticket second = manager.register(Collections.singletonList(key));
        BlockingManager.Ticket third = manager.register(Collections.singletonList(key));

        first.close();
        assertThat(second.isFirst(key)).isTrue();
        assertThat(third.isFirst(key)).isFalse();

        second.close();
        assertThat(third.isFirst(key)).isTrue();
    }

    @Test
    void orderingIsTrackedPerKey() {
        Slice a = create("a");
        Slice b = create("b");
        //first blocks on a only; second blocks on a and b.
        BlockingManager.Ticket first = manager.register(Collections.singletonList(a));
        BlockingManager.Ticket second = manager.register(Arrays.asList(a, b));

        //On a, first is older. On b, second is the only (hence oldest) waiter.
        assertThat(first.isFirst(a)).isTrue();
        assertThat(second.isFirst(a)).isFalse();
        assertThat(second.isFirst(b)).isTrue();

        //Once first leaves a, second leads on a too.
        first.close();
        assertThat(second.isFirst(a)).isTrue();
    }

    @Test
    void unregisteredKeyDefaultsToFirst() {
        //Defensive contract: a waiter is never blocked on a key it isn't
        //tracked for (e.g. it registered on "a" but probes "other").
        Slice tracked = create("a");
        Slice untracked = create("other");
        BlockingManager.Ticket ticket = manager.register(Collections.singletonList(tracked));

        assertThat(ticket.isFirst(untracked)).isTrue();
    }

    @Test
    void afterAllWaitersLeaveKeyAnyTicketIsFirstAgain() {
        Slice key = create("a");
        BlockingManager.Ticket first = manager.register(Collections.singletonList(key));
        BlockingManager.Ticket second = manager.register(Collections.singletonList(key));

        first.close();
        second.close();

        //No one is tracked for the key anymore: a brand-new waiter leads.
        BlockingManager.Ticket third = manager.register(Collections.singletonList(key));
        assertThat(third.isFirst(key)).isTrue();
        third.close();
    }

    @Test
    void closeIsIdempotent() {
        BlockingManager.Ticket ticket = manager.register(keys("a"));
        assertThat(manager.blockedClients()).isEqualTo(1);

        ticket.close();
        ticket.close();
        ticket.close();

        //Count must not go negative from repeated closes.
        assertThat(manager.blockedClients()).isZero();
    }

    @Test
    void closeDoesNotAffectOtherWaitersOnSharedKey() {
        Slice key = create("a");
        BlockingManager.Ticket first = manager.register(Collections.singletonList(key));
        BlockingManager.Ticket second = manager.register(Collections.singletonList(key));

        //Closing the same ticket twice must not corrupt the other waiter's standing.
        first.close();
        first.close();

        assertThat(manager.blockedClients()).isEqualTo(1);
        assertThat(second.isFirst(key)).isTrue();
    }

    @Test
    void worksWithTryWithResources() {
        Slice key = create("a");
        try (BlockingManager.Ticket ticket = manager.register(Collections.singletonList(key))) {
            assertThat(manager.blockedClients()).isEqualTo(1);
            assertThat(ticket.isFirst(key)).isTrue();
        }
        assertThat(manager.blockedClients()).isZero();
    }
}
