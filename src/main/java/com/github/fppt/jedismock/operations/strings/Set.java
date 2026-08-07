package com.github.fppt.jedismock.operations.strings;

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
 * <p>
 * GET replaces the OK with whatever the key held before, or a nil, and reports
 * it even when NX or XX then declines to write. Reading the old value is also
 * what makes WRONGTYPE possible: without GET, SET overwrites a key of any type.
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
        boolean get = chosen.containsKey(Group.RETURN);
        //Only now that the list is known to be well formed is the expiration
        //looked at, and it is checked before the key is even read
        long millis = 0;
        //Only the four timed options set a time, so the two are non-null together
        if (expiration != null && time != null) {
            try {
                millis = ExpirationArgument.millis(time.toString(), expiration.seconds,
                        expiration.absolute, base().getClock().millis(), self().value());
            } catch (IllegalArgumentException e) {
                return Response.error(e.getMessage());
            }
        }

        //GET is the one option that reads the key, and so the only reason SET
        //ever answers WRONGTYPE. It does so only once the expiration has been
        //accepted, and before NX or XX gets to decide anything
        Slice previous = get ? base().getSlice(key) : null;

        //For the decision itself only the key's presence matters, never its
        //type: a plain SET replaces a value of any type, so NX on a list
        //declines with a nil and XX overwrites it
        if (existence != null) {
            boolean required = existence == Option.XX;
            if (base().exists(key) != required) {
                //A declined write still reports the previous value under GET;
                //without it, and for an absent key, the reply is a nil
                return get ? Response.bulkString(previous) : Response.NULL;
            }
        }
        store(key, value.extract(), expiration, millis);
        return get ? Response.bulkString(previous) : Response.OK;
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

}
