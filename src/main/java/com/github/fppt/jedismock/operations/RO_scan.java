package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import com.google.common.collect.Lists;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.github.fppt.jedismock.Utils.convertToLong;

@RedisCommand("scan")
class RO_scan extends AbstractRedisOperation {

    private static final long CURSOR_START = 0;
    // From the Redis documentation, the default count if not specified:
    private static final long DEFAULT_COUNT = 10;

    private static final String MATCH = "match";
    private static final String COUNT = "count";

    protected Slice cursorSlice;
    protected int size;

    RO_scan(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    void doOptionalWork() {
        this.cursorSlice = params().get(0);
        this.size = base().keys().size();
    }

    Slice response() {
        long cursor = cursorSlice != null ? convertToLong(cursorSlice.toString()) : CURSOR_START;

        String match = extractParameter(params(), MATCH).map(Slice::toString).orElse("*");
        long count = extractParameter(params(), COUNT).map(s -> convertToLong(s.toString())).orElse(DEFAULT_COUNT);

        String regex = Utils.createRegexFromGlob(match);
        List<Slice> matchingValues = getMatchingValues(regex, cursor, count);

        cursor = cursor + count;
        if (cursor >= size) {
            cursor = CURSOR_START;
        }

        List<Slice> response = Lists.newArrayList(Response.bulkString(Slice.create(String.valueOf(cursor))), Response.array(matchingValues));
        return Response.array(response);
    }

    private static Optional<Slice> extractParameter(List<Slice> params, String name) {
        for (int i = 0; i < params.size(); i++) {
            String param = new String(params.get(i).data());
            if (name.equalsIgnoreCase(param)) {
                return Optional.of(params.get(i + 1));
            }
        }
        return Optional.empty();
    }

    protected List<Slice> getMatchingValues(String regex, long cursor, long count) {
        return base().keys().stream()
                .skip(cursor)
                .limit(count)
                .filter(x -> x.toString().matches(regex))
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }
}
