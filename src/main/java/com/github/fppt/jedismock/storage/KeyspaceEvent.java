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
    //Generic (g): not tied to one value type. Note that a container emptied by
    //its own type's command is reported with this class, not the type's.
    DEL("del", EventClass.GENERIC),
    EXPIRE("expire", EventClass.GENERIC),
    PERSIST("persist", EventClass.GENERIC),
    RENAME_FROM("rename_from", EventClass.GENERIC),
    RENAME_TO("rename_to", EventClass.GENERIC),
    MOVE_FROM("move_from", EventClass.GENERIC),
    MOVE_TO("move_to", EventClass.GENERIC),
    COPY_TO("copy_to", EventClass.GENERIC),

    //Expired (x)
    EXPIRED("expired", EventClass.EXPIRED),

    //String ($). Every flavour of assignment reports "set", and the
    //decrementing variants report "incrby" just like the incrementing ones.
    SET("set", EventClass.STRING),
    APPEND("append", EventClass.STRING),
    SETRANGE("setrange", EventClass.STRING),
    INCRBY("incrby", EventClass.STRING),
    INCRBYFLOAT("incrbyfloat", EventClass.STRING),

    //List (l). A multi-element push reports one event, and the conditional
    //(X) variants report the same event as their unconditional forms.
    LPUSH("lpush", EventClass.LIST),
    RPUSH("rpush", EventClass.LIST),
    LPOP("lpop", EventClass.LIST),
    RPOP("rpop", EventClass.LIST),
    LINSERT("linsert", EventClass.LIST),
    LSET("lset", EventClass.LIST),
    LREM("lrem", EventClass.LIST),
    LTRIM("ltrim", EventClass.LIST),
    SORTSTORE("sortstore", EventClass.LIST),

    //Set (s)
    SADD("sadd", EventClass.SET),
    SREM("srem", EventClass.SET),
    SPOP("spop", EventClass.SET),
    SINTERSTORE("sinterstore", EventClass.SET),
    SUNIONSTORE("sunionstore", EventClass.SET),
    SDIFFSTORE("sdiffstore", EventClass.SET),

    //Sorted set (z). Note ZINCRBY reports "zincr".
    ZADD("zadd", EventClass.ZSET),
    ZINCR("zincr", EventClass.ZSET),
    ZREM("zrem", EventClass.ZSET),
    ZREMRANGEBYSCORE("zremrangebyscore", EventClass.ZSET),
    ZREMRANGEBYRANK("zremrangebyrank", EventClass.ZSET),
    ZREMRANGEBYLEX("zremrangebylex", EventClass.ZSET),
    ZUNIONSTORE("zunionstore", EventClass.ZSET),
    ZINTERSTORE("zinterstore", EventClass.ZSET),
    ZDIFFSTORE("zdiffstore", EventClass.ZSET),
    ZRANGESTORE("zrangestore", EventClass.ZSET),
    ZPOPMIN("zpopmin", EventClass.ZSET),
    ZPOPMAX("zpopmax", EventClass.ZSET),

    //Hash (h). HSET, HMSET and HSETNX all report "hset".
    HSET("hset", EventClass.HASH),
    HDEL("hdel", EventClass.HASH),
    HINCRBY("hincrby", EventClass.HASH),
    HINCRBYFLOAT("hincrbyfloat", EventClass.HASH);

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
