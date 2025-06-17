package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.Utils.reservoirSampling;

@RedisCommand("srandmember")
public class SRandMember extends AbstractRedisOperation {
    public SRandMember(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    @Override
    protected Slice response() {
        RMSet set = base().getSet(params().get(0));
        boolean isArrayResponse = params().size() > 1;

        int number = isArrayResponse ? Utils.convertToInteger(params().get(1).toString()) : 1;
        if (set == null) {
            return isArrayResponse ? Response.EMPTY_ARRAY : Response.NULL;
        }

        List<Slice> result = selectEntries(set.getStoredData(), number, ThreadLocalRandom.current());
        return isArrayResponse ?
                Response.array(result.stream().map(Response::bulkString).collect(Collectors.toList())) :
                Response.bulkString(result.get(0));
    }

    private List<Slice> selectEntries(Set<Slice> set, int count, Random random) {
        if (count > 0) {
            return reservoirSampling(set, count, random);
        } else {
            List<Slice> list = new ArrayList<>(set);
            return ThreadLocalRandom.current().ints(-count, 0, list.size())
                    .mapToObj(list::get)
                    .collect(Collectors.toList());
        }
    }
}
