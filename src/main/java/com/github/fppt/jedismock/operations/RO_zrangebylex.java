package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RedisCommand("zrangebylex")
class RO_zrangebylex extends AbstractRedisOperation {

    static final String NEGATIVELY_INFINITE = "-";
    static final String POSITIVELY_INFINITE = "+";
    static final String INCLUSIVE_PREFIX = "[";
    static final String EXCLUSIVE_PREFIX = "(";

    RO_zrangebylex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);
        final RMHMap mapDBObj = getHMapFromBaseOrCreateEmpty(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        String start = params().get(1).toString();
        if (!validateStart(start)) {
            return buildErrorResponse("start");
        }

        String end = params().get(2).toString();
        if (!validateEnd(end)) {
            return buildErrorResponse("end");
        }

        return Response.array(doProcess(map, start, end));
    }

    private Slice buildErrorResponse(String param) {
        return Response.error("Valid " + param + " must start with '" + INCLUSIVE_PREFIX + "' or '"
                    + EXCLUSIVE_PREFIX + "' or unbounded");        
    }
    
    protected boolean validateStart(String start) {
        return getStartUnbounded().equals(start) || startsWithAnyPrefix(start);
    }

    protected boolean validateEnd(String end) {
        return getEndUnbounded().equals(end) || startsWithAnyPrefix(end);
    }
    
    protected boolean startsWithAnyPrefix(String s) {
        return s.startsWith(INCLUSIVE_PREFIX) || s.startsWith(EXCLUSIVE_PREFIX);
    }

    protected List<Slice> doProcess(Map<Slice, Double> map, String start, String end) {
        return map.keySet().stream()
                .filter(buildStartPredicate(start).and(buildEndPredicate(end)))
                .sorted()
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }

    protected Predicate<Slice> buildStartPredicate(String start) {
        return p -> getStartUnbounded().equals(start) ||
                (start.startsWith(INCLUSIVE_PREFIX)
                        ? p.toString().compareTo(start.substring(1)) >= 0
                        : p.toString().compareTo(start.substring(1)) > 0);
    }

    protected Predicate<Slice> buildEndPredicate(String end) {
        return p -> getEndUnbounded().equals(end) ||
                (end.startsWith(INCLUSIVE_PREFIX)
                        ? p.toString().compareTo(end.substring(1)) <= 0
                        : p.toString().compareTo(end.substring(1)) < 0);
    }

    protected String getStartUnbounded() {
        return NEGATIVELY_INFINITE;
    }

    protected String getEndUnbounded() {
        return POSITIVELY_INFINITE;
    }
}
