package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.datastructures.RMSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

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
        int number;
        if (params().size() > 1) {
            if (set == null) {
                return Response.EMPTY_ARRAY;
            }
            int result;
            String value = params().get(1).toString();
            try {
                result = Integer.parseInt(value);
            } catch (NumberFormatException e) {
                throw new WrongValueTypeException("ERR value is out of range");
            }
            number = result;
        } else {
            if (set == null) {
                return Response.NULL;
            }
            number = 1;
        }

        // TODO: more effective algorithms should be used here,
        // avoiding conversion of set to list, shuffling all the elements etc.
        List<Slice> list = new ArrayList<>(set.getStoredData());
        if (number == 1) {
            int index = ThreadLocalRandom.current().nextInt(list.size());
            return params().size() > 1 ?
                    Response.array(Response.bulkString(list.get(index))) :
                    Response.bulkString(list.get(index));
        } else if (number > 1) {
            Collections.shuffle(list);
            return Response.array(
                    list.stream()
                            .map(Response::bulkString)
                            .limit(number)
                            .collect(Collectors.toList()));
        } else {
            List<Slice> result =
                    ThreadLocalRandom.current().ints(-number, 0, list.size())
                            .mapToObj(list::get)
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
            return Response.array(result);
        }
    }
}
