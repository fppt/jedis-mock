package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.datastructures.Slice;

/**
 * Represents a Redis Operation which can be executed against {@link RedisBase}
 */
public interface RedisOperation {
    Slice execute();
}
