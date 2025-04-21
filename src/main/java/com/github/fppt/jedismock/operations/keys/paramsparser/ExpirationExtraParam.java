package com.github.fppt.jedismock.operations.keys.paramsparser;


import com.github.fppt.jedismock.datastructures.Slice;

import java.util.EnumSet;
import java.util.List;

public final class ExpirationExtraParam {
    private enum Options {
        XX, NX, LT, GT
    }

    private int index;
    private final EnumSet<Options> options = EnumSet.noneOf(Options.class);

    public ExpirationExtraParam(List<Slice> params, boolean expectExtra) throws ExpirationParamsException {
        param:
        for (index = 2; index < params.size(); index++) {
            String opt = params.get(index).toString();
            for (Options value : Options.values()) {
                if (value.toString().equalsIgnoreCase(opt)) {
                    options.add(value);
                    continue param;
                }
            }
            if (expectExtra) break;
            else throw new ExpirationParamsException("ERR Unsupported option " + opt);
        }
        if (options.contains(Options.NX) && options.size() > 1) {
            throw new ExpirationParamsException("ERR NX and XX, GT or LT options at the same time are not compatible");
        }
        if (options.contains(Options.GT) && options.contains(Options.LT)) {
            throw new ExpirationParamsException("ERR GT and LT options at the same time are not compatible");
        }
    }

    public int getIndex() {
        return index;
    }

    public boolean checkTiming(Long oldTTL, long newTTL) {
        //Treat empty or negative TTL as infinite
        if (oldTTL == null || oldTTL < 0) {
            oldTTL = Long.MAX_VALUE;
        }
        if (options.contains(Options.NX) && oldTTL != Long.MAX_VALUE) {
            return false;
        }
        if (options.contains(Options.XX) && oldTTL == Long.MAX_VALUE) {
            return false;
        }
        if (options.contains(Options.LT) && newTTL >= oldTTL) {
            return false;
        }
        return !options.contains(Options.GT) || newTTL > oldTTL;
    }
}
