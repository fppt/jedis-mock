package com.github.fppt.jedismock.storage;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A thin, server-wide {@code CONFIG} namespace: a plain key→value store that
 * lets {@code CONFIG GET}/{@code CONFIG SET} round-trip arbitrary parameters.
 * <p>
 * The goal is <em>not</em> to model real Redis configuration, but simply to stop
 * clients that issue {@code CONFIG} under the hood (e.g. Lettuce, Redisson) from
 * failing with "unsupported operation", while letting a value written with
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
    private final Map<String, String> values = new HashMap<>();

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
}
