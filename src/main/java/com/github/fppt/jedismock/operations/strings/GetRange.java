package com.github.fppt.jedismock.operations.strings;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

/**
 * Returns the inclusive substring between two offsets, either of which may be
 * negative to count back from the end. See {@link Substr} for the older name
 * of this same command.
 */
@RedisCommand(value = "getrange", arity = 4)
class GetRange extends AbstractGetRange {
    GetRange(RedisBase base, List<Slice> params) {
        super(base, params);
    }

}
