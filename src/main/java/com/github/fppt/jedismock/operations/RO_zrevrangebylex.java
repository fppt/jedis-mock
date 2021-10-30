package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;
import java.util.Comparator;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@RedisCommand("zrevrangebylex")
class RO_zrevrangebylex extends RO_zrangebylex {

    RO_zrevrangebylex(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected List<Slice> doProcess(Map<Slice, Double> map, String start, String end) {
        return map.keySet().stream()
                .filter(buildStartPredicate(start).and(buildEndPredicate(end)))
                .sorted(Comparator.reverseOrder())
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }

    @Override
    protected Predicate<Slice> buildStartPredicate(String start) {
        return p -> getStartUnbounded().equals(start) ||
                (start.startsWith(INCLUSIVE_PREFIX)
                        ? p.toString().compareTo(start.substring(1)) <= 0
                        : p.toString().compareTo(start.substring(1)) < 0);
    }

    @Override
    protected Predicate<Slice> buildEndPredicate(String end) {
        return p -> getEndUnbounded().equals(end) ||
                (end.startsWith(INCLUSIVE_PREFIX)
                        ? p.toString().compareTo(end.substring(1)) >= 0
                        : p.toString().compareTo(end.substring(1)) > 0);
    }
    
    @Override
    protected String getStartUnbounded() {
        return POSITIVELY_INFINITE;
    }

    @Override
    protected String getEndUnbounded() {
        return NEGATIVELY_INFINITE;
    }    
}
