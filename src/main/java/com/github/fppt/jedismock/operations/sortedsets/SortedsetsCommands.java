package com.github.fppt.jedismock.operations.sortedsets;

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
public final class SortedsetsCommands {

    private SortedsetsCommands() {
    }

    public static List<Class<? extends RedisOperation>> commands() {
        return Arrays.asList(
                BZMPop.class,
                BZPopMax.class,
                BZPopMin.class,
                ZAdd.class,
                ZCard.class,
                ZCount.class,
                ZDiff.class,
                ZDiffStore.class,
                ZIncrBy.class,
                ZInter.class,
                ZInterCard.class,
                ZInterStore.class,
                ZLexCount.class,
                ZMPop.class,
                ZMScore.class,
                ZPopMax.class,
                ZPopMin.class,
                ZRange.class,
                ZRangeByLex.class,
                ZRangeByScore.class,
                ZRangeStore.class,
                ZRank.class,
                ZRem.class,
                ZRemRangeByLex.class,
                ZRemRangeByRank.class,
                ZRemRangeByScore.class,
                ZRevRange.class,
                ZRevRangeByLex.class,
                ZRevRangeByScore.class,
                ZRevRank.class,
                ZScan.class,
                ZScore.class,
                ZUnion.class,
                ZUnionStore.class);
    }
}
