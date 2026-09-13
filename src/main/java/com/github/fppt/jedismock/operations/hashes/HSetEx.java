package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.datastructures.streams.StreamErrors.SYNTAX_ERROR;

@RedisCommand("hsetex")
class HSetEx extends AbstractRedisOperation {
    HSetEx(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 5;
    }

    protected Slice response() {
        Slice key = params().get(0);

        Params params = parseParams(params());

        if (params.condition != null) {
            RMHash hash = base().getHash(key);
            switch (params.condition) {
                case NONE_EXIST:
                    if (hash != null && params.fields.keySet().stream().anyMatch(hash::keyExists)) {
                        return Response.integer(0);
                    }
                    break;
                case ALL_EXIST:
                    if (hash == null || !params.fields.keySet().stream().allMatch(hash::keyExists)) {
                        return Response.integer(0);
                    }
                    break;
            }
        }

        for (Map.Entry<Slice, Slice> field : params.fields.entrySet()) {
            base().putSlice(key, field.getKey(), field.getValue(), params.ttl);
        }

        base().notifyKeyspaceEvent(KeyspaceEvent.HSET, key);
        // TODO: HEXPIRE/HDEL events

        return Response.integer(1);
    }

    private Params parseParams(List<Slice> params) {
        int currentIndex = 1;

        Long ttl = -1L;
        Params.Condition condition = null;
        Map<Slice, Slice> fields = null;

        while (currentIndex < params.size()) {
            Params.Option option = Params.Option.of(params.get(currentIndex++).toString());
            switch (option.group) {
                case FIELDS:
                    if (params.size() <= currentIndex + 2) {
                        throw new ArgumentException("ERR wrong number of arguments for 'hsetex' command");
                    }
                    int numFields = 0;
                    try {
                        numFields = Integer.parseUnsignedInt(params.get(currentIndex++).toString());
                    } catch (NumberFormatException ignored) {
                        // do nothing. Even when the number format is wrong, "real" Redis says
                        // "invalid number of fields".
                    }
                    if (numFields < 1) {
                        throw new ArgumentException("ERR invalid number of fields");
                    }
                    if (params.size() - currentIndex != 2 * numFields) {
                        throw new ArgumentException("ERR wrong number of arguments for 'hsetex' command");
                    }
                    fields = new HashMap<>(numFields);
                    for (int i = 0; i < numFields; i += 1) {
                        fields.put(params().get(currentIndex++), params().get(currentIndex++));
                    }
                    break;

                case EXISTENCE:
                    if (condition != null) {
                        throw new ArgumentException("ERR Only one of FXX or FNX arguments can be specified");
                    }
                    switch (option) {
                        case FNX:
                            condition = Params.Condition.NONE_EXIST;
                            break;
                        case FXX:
                            condition = Params.Condition.ALL_EXIST;
                            break;
                        default:
                            throw new ArgumentException(SYNTAX_ERROR);
                    }
                    break;

                case EXPIRATION:
                    if (ttl == null || ttl != -1) {
                        throw new ArgumentException("ERR Only one of EX, PX, EXAT, PXAT or KEEPTTL arguments can be specified");
                    }
                    if (option == Params.Option.KEEPTTL) {
                        ttl = null;
                    } else {
                        long now = base().getClock().millis();
                        long expiryMillis = millis(params.get(currentIndex++).toString(),
                                option.seconds, option.absolute, now);

                        switch (option) {
                            case EX:
                            case PX:
                                ttl = expiryMillis;
                                break;
                            case EXAT:
                            case PXAT:
                                ttl = expiryMillis - now;
                                break;
                            default:
                                throw new ArgumentException(SYNTAX_ERROR);
                        }
                    }
                    break;
            }
        }

        return new Params(condition, ttl, fields);
    }

    static long millis(String value, boolean seconds, boolean absolute, long now) {
        long parsed;
        try {
            parsed = Long.parseLong(value);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("ERR value is not an integer or out of range");
        }
        //A non-positive expiration is out of range for every unit, absolute ones included
        if (parsed < 0) {
            throw new IllegalArgumentException("ERR invalid expire time, must be >= 0");
        }
        //Seconds are rejected once they overflow milliseconds
        if (seconds && parsed > Long.MAX_VALUE / 1000) {
            throw invalidExpireTime();
        }
        long millis = seconds ? parsed * 1000 : parsed;
        //Only a relative expiration is later offset by the current time, so
        //only it can overflow; an absolute deadline is already the answer
        if (!absolute && millis >= Long.MAX_VALUE - now) {
            throw invalidExpireTime();
        }
        return millis;
    }

    static IllegalArgumentException invalidExpireTime() {
        return new IllegalArgumentException("ERR invalid expire time in 'hsetex' command");
    }

    private static class Params {
        final Condition condition;
        final Long ttl;
        final Map<Slice, Slice> fields;

        private Params(Condition condition, Long ttl, Map<Slice, Slice> fields) {
            this.condition = condition;
            this.ttl = ttl;
            this.fields = fields;
        }

        private enum Condition {
            NONE_EXIST,
            ALL_EXIST,
        }

        private enum Option {
            FIELDS(Group.FIELDS, false, false),
            FNX(Group.EXISTENCE, false, false),
            FXX(Group.EXISTENCE, false, false),
            EX(Group.EXPIRATION, true, false),
            PX(Group.EXPIRATION, false, false),
            EXAT(Group.EXPIRATION, true, true),
            PXAT(Group.EXPIRATION, false, true),
            KEEPTTL(Group.EXPIRATION, false, false),
            ;

            private final Group group;
            private final boolean seconds;
            private final boolean absolute;

            Option(Group group, boolean seconds, boolean absolute) {
                this.group = group;
                this.seconds = seconds;
                this.absolute = absolute;
            }

            static Option of(String name) {
                for (Option option : values()) {
                    if (option.name().equalsIgnoreCase(name)) {
                        return option;
                    }
                }
                throw new ArgumentException("ERR unknown argument: " + name);
            }

            private enum Group {
                FIELDS, EXISTENCE, EXPIRATION
            }
        }
    }
}
