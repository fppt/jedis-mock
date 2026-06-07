package com.github.fppt.jedismock.storage;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A thin, server-wide {@code CONFIG} namespace: a plain key→value store that
 * lets {@code CONFIG GET}/{@code CONFIG SET} round-trip arbitrary parameters.
 * <p>
 * The goal is <em>not</em> to model real Redis configuration, but simply to stop
 * code that issues {@code CONFIG} from failing with "unsupported operation" —
 * notably Spring Data Redis / Spring Session, which send
 * {@code CONFIG SET notify-keyspace-events} automatically when a key-expiration
 * listener is registered — while letting a value written with
 * {@code SET} be read back with {@code GET}. Stored values do not change how the
 * mock behaves; the few parameters that <em>do</em> affect behaviour (currently
 * {@code lua-time-limit}) are handled by their owning component, not here.
 * <p>
 * Keys are treated case-insensitively, matching Redis. Server-wide, so the value
 * is shared across all of a server's connections. Only the {@code CONFIG}
 * command touches it, and that runs under the shared data lock
 * ({@link OperationExecutorState#lock()}), so a plain {@link HashMap} suffices —
 * no internal synchronization is needed (unlike {@link ScriptingManager}, which
 * is read off-lock by the BUSY gate).
 */
public final class RedisConfiguration {
    /** Default {@code proto-max-bulk-len}, matching real Redis (512 MB). */
    private static final long DEFAULT_PROTO_MAX_BULK_LEN = 512L * 1024 * 1024;

    private final Map<String, String> values = new HashMap<>();
    private long protoMaxBulkLen = DEFAULT_PROTO_MAX_BULK_LEN;

    public void set(String key, String value) {
        values.put(key.toLowerCase(Locale.ROOT), value);
    }

    /**
     * @return the value previously {@link #set}, or an empty string if the key
     * was never set (mirroring how real Redis reports an unset string parameter).
     */
    public String get(String key) {
        return values.getOrDefault(key.toLowerCase(Locale.ROOT), "");
    }

    /**
     * @return the configured {@code proto-max-bulk-len} — the largest string a
     * command may build (enforced by {@code SETRANGE}/{@code APPEND}).
     */
    public long getProtoMaxBulkLen() {
        return protoMaxBulkLen;
    }

    /**
     * Set {@code proto-max-bulk-len}, capped at {@link Integer#MAX_VALUE}: the
     * mock stores strings as {@code byte[]}, so it cannot honour a larger limit
     * anyway. Reporting the cap back also makes real Redis's large-memory tests
     * (which set this to many GB and then check it round-tripped) self-skip on
     * the mock, exactly as they do on a 32-bit build.
     */
    public void setProtoMaxBulkLen(long protoMaxBulkLen) {
        this.protoMaxBulkLen = Math.min(protoMaxBulkLen, Integer.MAX_VALUE);
    }
}
