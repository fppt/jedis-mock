package com.github.fppt.jedismock.operations.lists;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import com.github.fppt.jedismock.datastructures.RMList;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import static java.lang.Math.abs;

@RedisCommand(value = "lpos", arity = -3)
public class LPos extends AbstractRedisOperation {

    private static final String RANK = "rank";
    private static final String COUNT = "count";
    private static final String MAXLEN = "maxlen";

    private int count;
    private int rank;
    private int maxLen;

    private boolean containsCount = false;

    public LPos(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        Slice element = params().get(1);

        RMList list = getListFromBaseOrCreateEmpty(key);

        List<Slice> storedData = list.getStoredData();

        parseArgs(storedData.size());
        List<Slice> result = new LinkedList<>();
        int matches = 0;

        int start = rank > 0 ? 0 : storedData.size() - 1;
        int add = rank > 0 ? 1 : -1;

        int checkedValues = 0;

        for (int i = start; i < storedData.size() && i >= 0; i += add) {

            if (maxLen <= checkedValues) {
                break;
            }
            ++checkedValues;

            if (!storedData.get(i).equals(element)) {
                continue;
            }
            matches++;

            if (matches >= abs(rank) && result.size() < count) {
                result.add(Response.integer(i));
            }
        }

        if (result.isEmpty()) {
            return Response.NULL;
        }

        return !containsCount ? result.get(0) : Response.array(result);
    }

    private void parseArgs(int listSize) {
        Map<String, Slice> args = getArgsWithValues(params(), new HashSet<>(Arrays.asList(RANK, COUNT, MAXLEN)));

        rank = Optional.ofNullable(args.get(RANK)).map(slice -> Integer.parseInt(slice.toString())).orElse(1);

        if (rank == 0) {
            throw new IllegalArgumentException("ERR RANK can't be zero:");
        }

        count = Optional.ofNullable(args.get(COUNT)).map(slice -> Integer.parseInt(slice.toString())).orElse(1);
        count = count == 0 ? listSize : count;
        containsCount = args.containsKey(COUNT);
        maxLen = Optional.ofNullable(args.get(MAXLEN)).map(slice -> Integer.parseInt(slice.toString())).orElse(0);
        maxLen = maxLen == 0 ? listSize : maxLen;
    }

    private static Map<String, Slice> getArgsWithValues(List<Slice> params, Set<String> argNames) {
        Map<String, Slice> argMap = new HashMap<>();
        for (int i = 0; i < params.size(); i++) {
            String parameter = params.get(i).toString().toLowerCase();
            if (argNames.contains(parameter)) {
                if (++i == params.size()) {
                    throw new IllegalArgumentException("Param " + parameter + " expected to have a value");
                }
                argMap.put(parameter, params.get(i));
            }
        }
        return argMap;
    }
}
