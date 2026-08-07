package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * The original (Redis 1.0) name of {@link GetRange}, deprecated since 2.0 but
 * still dispatched to the very same implementation.
 */
@RedisCommand("substr")
class Substr extends AbstractGetRange {
    Substr(RedisBase base, List<Slice> params) {
        super(base, params);
    }
}
