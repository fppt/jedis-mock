package com.github.fppt.jedismock.operations.sets;

import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.AbstractRedisOperation;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.List;

@RedisCommand("sintercard")
class SInterCard extends AbstractRedisOperation {
    private static final String LIMIT = "LIMIT";

    SInterCard(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    protected int minArgs() {
        return 2;
    }

    @Override
    protected Slice response() {
        long numKeys = parseInteger(params().get(0));
        if (numKeys < 1) {
            return Response.error("ERR numkeys should be greater than 0");
        }
        if (numKeys > params().size() - 1) {
            return Response.error("ERR Number of keys can't be greater than number of args");
        }
        int keyCount = (int) numKeys;

        //Everything after the keys must be LIMIT <count>, repeated; the last one wins
        long limit = 0;
        for (int i = keyCount + 1; i < params().size(); i += 2) {
            if (!LIMIT.equalsIgnoreCase(params().get(i).toString()) || i + 1 == params().size()) {
                return Response.error("ERR syntax error");
            }
            limit = parseInteger(params().get(i + 1));
            if (limit < 0) {
                return Response.error("ERR LIMIT can't be negative");
            }
        }

        //Arguments are fully validated before any key is touched, so a syntax
        //error is reported in preference to the WRONGTYPE a bad key would raise
        int cardinality = new SInter(base(), params().subList(1, keyCount + 1))
                .getIntersection().size();
        return Response.integer(limit == 0 ? cardinality : Math.min(cardinality, limit));
    }

    /**
     * Parses {@code param} the way Redis would, mapping anything unparseable to
     * -1. Both callers reject negatives anyway, so a malformed argument and an
     * out-of-range one produce the same reply — as they do on a real server.
     */
    private static long parseInteger(Slice param) {
        return Utils.parseRedisLong(param.toString()).orElse(-1);
    }
}
