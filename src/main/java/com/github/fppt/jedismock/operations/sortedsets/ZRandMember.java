package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.WrongValueTypeException;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.reservoirSampling;


@RedisCommand("zrandmember")
public class ZRandMember extends AbstractRedisOperation {

    public ZRandMember(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 1;
    }

    @Override
    protected int maxArgs() {
        return 3;
    }

    @Override
    protected Slice response() {
        RMZSet set = base().getZSet(params().get(0));
        boolean isArrayResponse = params().size() > 1;

        int count = parseCount();
        boolean withScores = parseWithScores();
        if (set == null) {
            return isArrayResponse ? Response.EMPTY_ARRAY : Response.NULL;
        }

        List<ZSetEntry> selectedEntries = selectEntries(set, count);
        return isArrayResponse
                ? buildArrayResponse(selectedEntries, withScores)
                : buildSingleResponse(selectedEntries.get(0));
    }

    private int parseCount() {
        if (params().size() >= 2) {
            return Utils.convertToInteger(params().get(1).toString());
        }
        return 1;
    }

    private boolean parseWithScores() {
        if (params().size() == 3) {
            if ("withscores".equalsIgnoreCase(params().get(2).toString())) {
                return true;
            } else {
                throw new WrongValueTypeException("ERR syntax error");
            }
        }
        return false;
    }

    private List<ZSetEntry> selectEntries(RMZSet set, int count) {
        if (count > 0) {
            return reservoirSampling(set.entries(false), count, ThreadLocalRandom.current());
        } else {
            List<ZSetEntry> entries = new ArrayList<>(set.entries(false));
            return ThreadLocalRandom.current()
                    .ints(-count, 0, entries.size())
                    .mapToObj(entries::get)
                    .collect(Collectors.toList());
        }
    }

    private Slice buildArrayResponse(List<ZSetEntry> entries, boolean withScores) {
        return Response.array(entries.stream()
                .flatMap(e -> withScores
                        ? Stream.of(e.getValue(), Slice.create(String.valueOf(Math.round(e.getScore()))))
                        : Stream.of(e.getValue()))
                .map(Response::bulkString)
                .collect(Collectors.toList()));
    }

    private Slice buildSingleResponse(ZSetEntry entry) {
        return Response.bulkString(entry.getValue());
    }
}
