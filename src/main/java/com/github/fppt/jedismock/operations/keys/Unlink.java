package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand(value = "unlink", arity = -2, readOnly = false)
public class Unlink extends Del {
    Unlink(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    /*Note: this command behaves exactly like 'DEL'*/
}
