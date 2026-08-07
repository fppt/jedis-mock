package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.EnumMap;
import java.util.List;

/**
 * {@code SET key value [NX | XX] [GET] [EX seconds | PX milliseconds |
 * EXAT unix-time-seconds | PXAT unix-time-milliseconds | KEEPTTL]}.
 * <p>
 * The option list is read in a single left-to-right pass, and anything
 * unrecognised, an option contradicting one already seen, or an expiration
 * with nothing following it is a syntax error. That whole pass completes
 * before the expiration value is converted, which is observable: {@code SET k
 * v EX notanumber} reports the bad conversion, but {@code SET k v EX
 * notanumber badoption} reports the syntax error instead.
 */
@RedisCommand("set")
class Set extends AbstractRedisOperation {
    private static final String SYNTAX_ERROR = "ERR syntax error";

    Set(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    /**
     * Alternatives that exclude one another. Two <em>different</em> options
     * from one group are a syntax error; repeating the same one is accepted
     * and the last occurrence wins, just as in Redis's own argument parser.
     * GET sits in a group of its own precisely because it conflicts with
     * nothing.
     */
    private enum Group {
        EXISTENCE, RETURN, EXPIRATION
    }

    private enum Option {
        NX(Group.EXISTENCE, false, false, false),
        XX(Group.EXISTENCE, false, false, false),
        GET(Group.RETURN, false, false, false),
        KEEPTTL(Group.EXPIRATION, false, false, false),
        EX(Group.EXPIRATION, true, true, false),
        PX(Group.EXPIRATION, true, false, false),
        EXAT(Group.EXPIRATION, true, true, true),
        PXAT(Group.EXPIRATION, true, false, true);

        private final Group group;
        private final boolean takesTime;
        private final boolean seconds;
        private final boolean absolute;

        Option(Group group, boolean takesTime, boolean seconds, boolean absolute) {
            this.group = group;
            this.takesTime = takesTime;
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
        Slice value = params().get(1);

        EnumMap<Group, Option> chosen = new EnumMap<>(Group.class);
        Slice time = null;
        for (int i = 2; i < params().size(); i++) {
            Option option = Option.of(params().get(i).toString());
            if (option == null) {
                return Response.error(SYNTAX_ERROR);
            }
            Option previous = chosen.put(option.group, option);
            if (previous != null && previous != option) {
                return Response.error(SYNTAX_ERROR);
            }
            if (option.takesTime) {
                if (i + 1 >= params().size()) {
                    return Response.error(SYNTAX_ERROR);
                }
                time = params().get(++i);
            }
        }

        Option existence = chosen.get(Group.EXISTENCE);
        Option expiration = chosen.get(Group.EXPIRATION);
        //Only now that the list is known to be well formed is the expiration
        //looked at, and it is checked before the key is even read
        long millis = 0;
        //Only the four timed options set a time, so the two are non-null together
        if (expiration != null && time != null) {
            try {
                millis = parseAndValidate(time.toString(), expiration.seconds ? 1000 : 1);
            } catch (IllegalArgumentException e) {
                return Response.error(e.getMessage());
            }
        }

        //Raises WRONGTYPE for a non-string key
        boolean exists = existence != null && base().getSlice(key) != null;
        if (existence == Option.NX && exists || existence == Option.XX && !exists) {
            return Response.NULL;
        }
        store(key, value.extract(), expiration, millis);
        return Response.OK;
    }

    private void store(Slice key, RMDataStructure value, Option expiration, long millis) {
        if (expiration == Option.KEEPTTL) {
            Long deadline = base().getDeadline(key);
            base().putValue(key, value);
            if (deadline != null) {
                base().setDeadline(key, deadline);
            }
        } else if (expiration == null) {
            base().putValue(key, value);
        } else if (expiration.absolute) {
            base().putValue(key, value);
            base().setDeadline(key, millis);
        } else {
            base().putValue(key, value, millis);
        }
        //Reports the assignment, then — when the command also set an
        //expiration — the generic 'expire', in that order, as real Redis does
        base().notifyKeyspaceEvent(KeyspaceEvent.SET, key);
        if (expiration != null && expiration != Option.KEEPTTL) {
            base().notifyKeyspaceEvent(KeyspaceEvent.EXPIRE, key);
        }
    }

    private long parseAndValidate(String param, int multiplier) {
        long value = Utils.convertToLong(param);
        if (value <= 0) {
            throw invalidExpireTime();
        }
        try {
            value = Math.multiplyExact(multiplier, value);
            Math.addExact(base().getClock().millis(), value);
        } catch (ArithmeticException e) {
            throw invalidExpireTime();
        }
        return value;
    }

    private IllegalArgumentException invalidExpireTime() {
        return new IllegalArgumentException(
                String.format("ERR invalid expire time in '%s' command", self().value()));
    }
}
