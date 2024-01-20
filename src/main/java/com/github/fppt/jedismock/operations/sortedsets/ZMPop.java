package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.EnumSet;
import java.util.List;

@RedisCommand("zmpop")
public class ZMPop extends ZPop {
    enum Options {
        MIN, MAX, COUNT
    }

    private final EnumSet<Options> options = EnumSet.noneOf(Options.class);
    private int numKeys = 1;
    private long count = 1;

    ZMPop(RedisBase base, List<Slice> params) {
        super(base, params, false);
    }

    @Override
    protected int minArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        parseArgs();
        return getResult();
    }

    protected Slice getResult() {
        for (int i = 0; i < numKeys; i++) {
            Slice key = params().get(i + 1);
            RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
            if (!mapDBObj.isEmpty()) {
                List<Slice> newParams = new ArrayList<>();
                newParams.add(key);
                newParams.add(Slice.create(String.valueOf(Math.min(count, mapDBObj.size()))));
                List<Slice> result = new ZPop(base(), newParams, options.contains(Options.MAX)).pop();

                List<Slice> popedList = new ArrayList<>();
                for (int index = 0; index < result.size(); index += 2) {
                    Slice value = result.get(index);
                    Slice score = result.get(index + 1);
                    popedList.add(Response.array(Arrays.asList(value, score)));
                }
                Slice pop = Response.array(popedList);
                return Response.array(Arrays.asList(Response.bulkString(key), pop));
            }
        }
        return Response.NULL_ARRAY;
    }

    protected final List<Slice> parseArgs() {
        List<Slice> temp = new ArrayList<>(params());
        for (Slice param : temp) {
            if (Options.MIN.toString().equalsIgnoreCase(param.toString())) {
                if (options.contains(Options.MAX)) {
                    throw new ArgumentException("ERR syntax error*");
                }
                options.add(Options.MIN);
                params().remove(param);
            } else if (Options.MAX.toString().equalsIgnoreCase(param.toString())) {
                if (options.contains(Options.MIN)) {
                    throw new ArgumentException("ERR syntax error*");
                }
                options.add(Options.MAX);
                params().remove(param);
            } else if (Options.COUNT.toString().equalsIgnoreCase(param.toString())) {
                if (options.contains(Options.COUNT)) {
                    throw new ArgumentException("ERR syntax error*");
                }
                options.add(Options.COUNT);
                int index = params().indexOf(param);
                try {
                    count = Long.parseLong(params().get(index + 1).toString());
                } catch (IndexOutOfBoundsException e) {
                    throw new ArgumentException("ERR syntax error*");
                } catch (NumberFormatException e) {
                    throw new ArgumentException("ERR count*");
                }
                if (count < 1) {
                    throw new ArgumentException("ERR count*");
                }
                params().remove(index + 1);
                params().remove(param);
            }
        }
        try {
            numKeys = Integer.parseInt(params().get(0).toString());
        } catch (NumberFormatException e) {
            throw new ArgumentException("ERR numkeys*");
        }
        if (numKeys < 1) {
            throw new ArgumentException("ERR numkeys*");
        }
        if (params().size() != numKeys + 1) {
            throw new ArgumentException("ERR syntax error*");
        }
        return params();
    }
}
