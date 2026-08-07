package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * {@code GETEX key [EX seconds | PX milliseconds | EXAT unix-time-seconds |
 * PXAT unix-time-milliseconds | PERSIST]}: a GET that may also change the
 * key's expiration, and nothing else — the value itself is never written.
 * <p>
 * The order in which failures are reported follows real Redis and is
 * observable: the option is parsed first, so a syntax error is reported even
 * for a missing key or one of the wrong type; the expiration value, in
 * contrast, is only validated after the key has been found to exist and to
 * hold a string, so {@code GETEX missing EX 0} replies nil rather than
 * erroring.
 */
@RedisCommand("getex")
class GetEx extends AbstractRedisOperation {

    GetEx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    /**
     * The mutually exclusive expiration options. Repeating the <em>same</em>
     * option is accepted (the last value wins), just as in Redis's own
     * argument parser; mixing two different ones is a syntax error.
     */
    private enum Option {
        EX(true, false),
        PX(false, false),
        EXAT(true, true),
        PXAT(false, true),
        PERSIST(false, false);

        private final boolean seconds;
        private final boolean absolute;

        Option(boolean seconds, boolean absolute) {
            this.seconds = seconds;
            this.absolute = absolute;
        }

        static Option of(String name) {
            for (Option option : values()) {
                if (option.name().equalsIgnoreCase(name)) {
                    return option;
                }
            }
            return null;
        }
    }

    protected Slice response() {
        Slice key = params().get(0);
        Option option = null;
        Slice time = null;
        for (int i = 1; i < params().size(); i++) {
            Option parsed = Option.of(params().get(i).toString());
            if (parsed == null || option != null && option != parsed) {
                return Response.error("ERR syntax error");
            }
            if (parsed != Option.PERSIST) {
                if (i + 1 >= params().size()) {
                    return Response.error("ERR syntax error");
                }
                time = params().get(++i);
            }
            option = parsed;
        }

        //Raises WRONGTYPE for a non-string key, which outranks any bad expiration
        Slice value = base().getSlice(key);
        if (value == null) {
            return Response.NULL;
        }

        if (time == null) {
            if (option == Option.PERSIST && base().setDeadline(key, -1) == 1) {
                base().notifyKeyspaceEvent(KeyspaceEvent.PERSIST, key);
            }
            return Response.bulkString(value);
        }

        long deadline;
        try {
            deadline = deadline(option, time);
        } catch (IllegalArgumentException e) {
            return Response.error(e.getMessage());
        }
        if (option.absolute && deadline <= base().getClock().millis()) {
            //An absolute deadline that has already passed drops the key straight
            //away, and is reported as a 'del' rather than an 'expired'
            base().deleteValue(key);
            base().notifyKeyspaceEvent(KeyspaceEvent.DEL, key);
        } else {
            base().setDeadline(key, deadline);
            base().notifyKeyspaceEvent(KeyspaceEvent.EXPIRE, key);
        }
        return Response.bulkString(value);
    }

    /** The absolute deadline in milliseconds, rejecting values Redis refuses. */
    private long deadline(Option option, Slice time) {
        long now = base().getClock().millis();
        long millis = ExpirationArgument.millis(
                time.toString(), option.seconds, option.absolute, now, self().value());
        return option.absolute ? millis : millis + now;
    }
}
