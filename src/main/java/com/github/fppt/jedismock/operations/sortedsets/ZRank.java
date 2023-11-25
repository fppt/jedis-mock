package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@RedisCommand("zrank")
public class ZRank extends AbstractByScoreOperation {
    private static final String WITH_SCORES = "WITHSCORE";
    private static final String IS_REV = "REV";

    private boolean withScores = false;
    private boolean isRev = false;

    ZRank(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected Slice response() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        Slice member = params().get(1);

        parseArgs();

        if (!mapDBObj.hasMember(member)) {
            return withScores
                    ? Response.NULL_ARRAY
                    : Response.NULL;
        }

        int rank = mapDBObj.entries(isRev)
                .headSet(new ZSetEntry(mapDBObj.getScore(member), member)).size();
        return withScores
                ? Response.array(Stream.of(Response.integer(rank),
                            Response.integer(Math.round(mapDBObj.getScore(member))))
                            .collect(Collectors.toList()))
                : Response.integer(rank);
    }

    private void parseArgs() {
        for (Slice param : params()) {
            if (WITH_SCORES.equalsIgnoreCase(param.toString())) {
                withScores = true;
            }
            if (IS_REV.equalsIgnoreCase(param.toString())) {
                isRev = true;
            }
        }
    }
}
