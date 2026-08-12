package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.operations.RedisOperation;

import java.util.Arrays;
import java.util.List;

/**
 * Registry of the {@link com.github.fppt.jedismock.operations.RedisCommand}
 * classes declared in this package.
 *
 * <p>Internal API. It exists so that
 * {@link com.github.fppt.jedismock.operations.CommandFactory} can enumerate
 * operations without scanning the classpath. Most operation classes are
 * package-private and can only be named from inside their own package, which
 * is why there is one registry per package rather than one central list.
 *
 * <p>Add an entry here when you add a command;
 * {@code CommandRegistryCompletenessTest} tells you exactly what is missing
 * if you forget.
 */
public final class KeysCommands {

    private KeysCommands() {
    }

    public static List<Class<? extends RedisOperation>> commands() {
        return Arrays.asList(
                Copy.class,
                Del.class,
                Exists.class,
                Expire.class,
                ExpireAt.class,
                ExpireTime.class,
                Keys.class,
                Move.class,
                Persist.class,
                PExpire.class,
                PExpireAt.class,
                PExpireTime.class,
                PTTL.class,
                Rename.class,
                Scan.class,
                TTL.class,
                Type.class,
                Unlink.class);
    }
}
