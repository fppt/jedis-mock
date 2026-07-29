package com.github.fppt.jedismock.storage;

import com.github.fppt.jedismock.storage.KeyspaceNotificationOptions.EventClass;

/**
 * A keyspace-notification event: the name Redis publishes for it and the
 * event class that {@code notify-keyspace-events} enables it with.
 * <p>
 * Every event belongs to exactly one class, so the two travel together here
 * rather than being passed side by side — a caller cannot pair an event with
 * the wrong class. The published name is held explicitly because it is part of
 * the wire protocol and does not always match the Java constant (for instance
 * {@code xgroup-create}, which is not a valid identifier).
 */
public enum KeyspaceEvent {
    DEL("del", EventClass.GENERIC),
    EXPIRE("expire", EventClass.GENERIC),
    PERSIST("persist", EventClass.GENERIC),
    RENAME_FROM("rename_from", EventClass.GENERIC),
    RENAME_TO("rename_to", EventClass.GENERIC),
    MOVE_FROM("move_from", EventClass.GENERIC),
    MOVE_TO("move_to", EventClass.GENERIC),
    COPY_TO("copy_to", EventClass.GENERIC),
    EXPIRED("expired", EventClass.EXPIRED);

    private final String eventName;
    private final EventClass eventClass;

    KeyspaceEvent(String eventName, EventClass eventClass) {
        this.eventName = eventName;
        this.eventClass = eventClass;
    }

    /** The event name as published, e.g. {@code rename_from}. */
    public String eventName() {
        return eventName;
    }

    public EventClass eventClass() {
        return eventClass;
    }
}
