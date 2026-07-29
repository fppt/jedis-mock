package com.github.fppt.jedismock.storage;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Set;

/**
 * The parsed value of the {@code notify-keyspace-events} configuration
 * parameter: which keyspace-notification event classes are enabled, and on
 * which of the two channel families ({@code K} = {@code __keyspace@<db>__:<key>},
 * {@code E} = {@code __keyevent@<db>__:<event>}) they are published.
 * <p>
 * Immutable; {@link #parse} and {@link #format} mirror real Redis exactly,
 * including the {@code A} alias, the canonical character order reported by
 * {@code CONFIG GET}, and the error message for an unknown character
 * (pinned by {@code NotifyKeyspaceEventsConfigTest} against real Redis).
 */
public final class KeyspaceNotificationOptions {

    /**
     * Event classes, declared in the canonical order Redis uses when
     * formatting the flag string: the classes covered by the {@code A} alias
     * ({@link #GENERIC} through {@link #MODULE}), then {@link #NEW},
     * {@link #KEYSPACE}, {@link #KEYEVENT} and {@link #KEY_MISS}.
     */
    public enum EventClass {
        GENERIC('g'),
        STRING('$'),
        LIST('l'),
        SET('s'),
        HASH('h'),
        ZSET('z'),
        EXPIRED('x'),
        EVICTED('e'),
        STREAM('t'),
        MODULE('d'),
        NEW('n'),
        KEYSPACE('K'),
        KEYEVENT('E'),
        KEY_MISS('m');

        private final char code;

        EventClass(char code) {
            this.code = code;
        }

        public char code() {
            return code;
        }
    }

    public static final KeyspaceNotificationOptions DISABLED =
            new KeyspaceNotificationOptions(EnumSet.noneOf(EventClass.class));

    /** The classes the {@code A} alias stands for: everything except K, E, n and m. */
    private static final Set<EventClass> ALL_ALIAS =
            Collections.unmodifiableSet(EnumSet.range(EventClass.GENERIC, EventClass.MODULE));

    private final Set<EventClass> classes;

    private KeyspaceNotificationOptions(EnumSet<EventClass> classes) {
        this.classes = Collections.unmodifiableSet(classes);
    }

    /**
     * @throws IllegalArgumentException on an unknown character, with the exact
     * message real Redis embeds in the {@code CONFIG SET} error reply
     */
    public static KeyspaceNotificationOptions parse(String flags) {
        EnumSet<EventClass> classes = EnumSet.noneOf(EventClass.class);
        nextCharacter:
        for (char c : flags.toCharArray()) {
            if (c == 'A') {
                classes.addAll(ALL_ALIAS);
                continue;
            }
            for (EventClass eventClass : EventClass.values()) {
                if (eventClass.code == c) {
                    classes.add(eventClass);
                    continue nextCharacter;
                }
            }
            throw new IllegalArgumentException("Invalid event class character. Use 'Ag$lshzxeKEtmdn'.");
        }
        return new KeyspaceNotificationOptions(classes);
    }

    public boolean isEnabled(EventClass eventClass) {
        return classes.contains(eventClass);
    }

    public boolean isEnabled(KeyspaceEvent event) {
        return isEnabled(event.eventClass());
    }

    /**
     * The canonical form reported by {@code CONFIG GET}: the {@code A} alias
     * collapses the classes it covers, followed by the remaining flags in
     * declaration order.
     */
    public String format() {
        StringBuilder result = new StringBuilder();
        if (classes.containsAll(ALL_ALIAS)) {
            result.append('A');
            for (EventClass eventClass : EnumSet.range(EventClass.NEW, EventClass.KEY_MISS)) {
                if (classes.contains(eventClass)) {
                    result.append(eventClass.code);
                }
            }
        } else {
            for (EventClass eventClass : classes) {
                result.append(eventClass.code);
            }
        }
        return result.toString();
    }
}
