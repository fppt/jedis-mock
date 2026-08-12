package com.github.fppt.jedismock.operations.lists;

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
public final class ListsCommands {

    private ListsCommands() {
    }

    public static List<Class<? extends RedisOperation>> commands() {
        return Arrays.asList(
                BLPop.class,
                BRPop.class,
                BRPopLPush.class,
                LIndex.class,
                LInsert.class,
                LLen.class,
                LPop.class,
                LPos.class,
                LPush.class,
                LPushX.class,
                LRange.class,
                LRem.class,
                LSet.class,
                LTrim.class,
                RPop.class,
                RPopLPush.class,
                RPush.class,
                RPushX.class,
                Sort.class);
    }
}
