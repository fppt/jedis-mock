package com.github.fppt.jedismock.operations.functions;

/**
 * The flags a library function may declare through
 * {@code redis.register_function{flags={...}}}. Declared in the order real
 * Redis defines them, which is also the order {@code FUNCTION LIST} reports
 * them in, whatever order the library gave them.
 * <p>
 * Only {@link #NO_WRITES} changes behaviour here: it makes the function run
 * read-only (and callable through {@code FCALL_RO}). The others concern
 * features jedis-mock does not model (memory limits, replication, cluster)
 * and are accepted and reported but otherwise ignored.
 */
public enum FunctionFlag {
    NO_WRITES("no-writes"),
    ALLOW_OOM("allow-oom"),
    ALLOW_STALE("allow-stale"),
    NO_CLUSTER("no-cluster"),
    ALLOW_CROSS_SLOT_KEYS("allow-cross-slot-keys");

    private final String flagName;

    FunctionFlag(String flagName) {
        this.flagName = flagName;
    }

    /**
     * @return the name as given in a library and reported by {@code FUNCTION LIST}
     */
    public String flagName() {
        return flagName;
    }

    /**
     * @return the flag with this name, matched case-insensitively like real
     * Redis does, or {@code null} if there is none
     */
    public static FunctionFlag lookup(String name) {
        for (FunctionFlag flag : values()) {
            if (flag.flagName.equalsIgnoreCase(name)) {
                return flag;
            }
        }
        return null;
    }
}
