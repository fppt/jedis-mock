package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.datastructures.ZSetEntryBound;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.NavigableSet;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static com.github.fppt.jedismock.Utils.convertToLong;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.LIMIT;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.REV;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.WITHSCORES;
import static com.github.fppt.jedismock.operations.sortedsets.AbstractZRange.Options.values;
import static java.util.Collections.emptyNavigableSet;

abstract class AbstractZRange extends AbstractByScoreOperation {

    enum Options {
        WITHSCORES, REV, BYSCORE, BYLEX, LIMIT
    }

    protected static final String EXCLUSIVE_PREFIX = "(";
    protected static final String LOWEST_POSSIBLE_SCORE = "-inf";
    protected static final String HIGHEST_POSSIBLE_SCORE = "+inf";
    protected final EnumSet<Options> options = EnumSet.noneOf(Options.class);
    protected int startIndex;
    protected int endIndex;
    protected long offset = 0;
    protected long count = 0;
    protected Slice key;
    protected RMZSet mapDBObj;

    AbstractZRange(RedisBase base, List<Slice> params) {
        super(base, params);
        parseArgs();
    }

    protected abstract ZSetEntryBound getStartBound(Slice start);
    protected abstract ZSetEntryBound getEndBound(Slice end);

    protected NavigableSet<ZSetEntry> getRange(ZSetEntryBound start, ZSetEntryBound end) {
        if (mapDBObj.isEmpty()) {
            return emptyNavigableSet();
        }

        NavigableSet<ZSetEntry> subset =
                mapDBObj.subset(start, end);
        if (options.contains(REV)) {
            subset = subset.descendingSet();
        }
        return subset;

    }

    protected Slice getSliceFromRange(NavigableSet<ZSetEntry> entries) {
        final List<Slice> list;
        if (options.contains(LIMIT)) {
            if (count == -1) {
                if (options.contains(WITHSCORES)) {
                    list = entries.stream()
                            .skip(offset)
                            .flatMap(e -> Stream.of(e.getValue(),
                                    Slice.create(String.format("%.0f", e.getScore()))))
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                } else {
                    list = entries.stream()
                            .skip(offset)
                            .map(ZSetEntry::getValue)
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                }
            } else {
                if (options.contains(WITHSCORES)) {
                    list = entries.stream()
                            .skip(offset)
                            .limit(count)
                            .flatMap(e -> Stream.of(e.getValue(),
                                    Slice.create(String.format("%.0f", e.getScore()))))
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                } else {
                    list = entries.stream()
                            .skip(offset)
                            .limit(count)
                            .map(ZSetEntry::getValue)
                            .map(Response::bulkString)
                            .collect(Collectors.toList());
                }

            }
        } else {
            if (options.contains(WITHSCORES)) {
                list = entries.stream()
                        .flatMap(e -> Stream.of(e.getValue(),
                                Slice.create(String.format("%.0f", e.getScore()))))
                        .map(Response::bulkString)
                        .collect(Collectors.toList());
            } else {
                list = entries.stream()
                        .map(ZSetEntry::getValue)
                        .map(Response::bulkString)
                        .collect(Collectors.toList());
            }
        }

        return Response.array(list);
    }

    protected final void parseArgs() {
        for (Slice param : params()) {
            for (Options value : values()) {
                if (value.toString().equalsIgnoreCase(param.toString())){
                    options.add(value);
                    break;
                }
            }

            if (LIMIT.toString().equalsIgnoreCase(param.toString())) {
                int index = params().indexOf(param);
                offset = convertToLong(params().get(++index).toString());
                count = convertToLong(params().get(++index).toString());
            }
        }
    }

    protected Slice remRangeFromKey(NavigableSet<ZSetEntry> entries) {
        int count = 0;
        for (ZSetEntry entry : new ArrayList<>(entries)) {
            mapDBObj.remove(entry.getValue());
            count++;
        }
        if (mapDBObj.isEmpty()) {
            base().deleteValue(key);
        } else {
            base().putValue(key, mapDBObj);
        }
        return Response.integer(count);
    }

    protected final void expectNoOptions() {
        if (!options.isEmpty()) {
            throw new ArgumentException("*syntax*");
        }
    }
}
