package com.github.fppt.jedismock.operations.keys;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;

@RedisCommand("keys")
class Keys extends AbstractRedisOperation {
    Keys(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    protected Slice response() {
        List<Slice> matchingKeys = new ArrayList<>();
        String regex = Utils.createRegexFromGlob(new String(params().get(0).data()));

        base().keys().forEach(keyData -> {
            String key = new String(keyData.data());
            if(key.matches(regex)){
                matchingKeys.add(Response.bulkString(keyData));
            }
        });

        return Response.array(matchingKeys);
    }
}
