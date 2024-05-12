package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.OperationExecutorState;

import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;

import static com.github.fppt.jedismock.operations.sortedsets.ZAdd.Options.CH;
import static com.github.fppt.jedismock.operations.sortedsets.ZAdd.Options.GT;
import static com.github.fppt.jedismock.operations.sortedsets.ZAdd.Options.INCR;
import static com.github.fppt.jedismock.operations.sortedsets.ZAdd.Options.LT;
import static com.github.fppt.jedismock.operations.sortedsets.ZAdd.Options.NX;
import static com.github.fppt.jedismock.operations.sortedsets.ZAdd.Options.XX;

@RedisCommand("zadd")
class ZAdd extends AbstractByScoreOperation {

    enum Options {
        XX, NX, LT, GT, CH, INCR
    }
    private final Object lock;

    private final EnumSet<Options> options = EnumSet.noneOf(Options.class);

    private int countAdd = 0;
    private int countChange = 0;

    ZAdd(OperationExecutorState state, List<Slice> params) {
        super(state.base(), params);
        this.lock = state.lock();
    }

    @Override
    protected Slice response() {
        parseParams();

        if (options.contains(NX) && (
                options.contains(GT) || options.contains(LT) || options.contains(XX))) {
            throw new ArgumentException("ERR syntax error");
        }

        if (options.contains(LT) && options.contains(GT)) {
            throw new ArgumentException("ERR syntax error");
        }

        return options.contains(INCR) ? incr() : adding();

    }

    private Slice incr() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (params().size() != 3) {
            throw new ArgumentException("ERR INCR option supports a single increment-element pair");
        }
        String increment = params().get(1).toString();
        Slice member = params().get(2);
        double score = (mapDBObj.getScore(member) == null) ? 0d :
                mapDBObj.getScore(member);

        double newScore = getSum(score, increment);

        if (newScore != score) {
            addOneElement(mapDBObj, member, newScore);
            if (countChange + countAdd > 0) {
                mapDBObj.put(member, newScore);
                base().putValue(key, mapDBObj);
                lock.notifyAll();
                return Response.bulkString(Slice.create(String.valueOf(newScore)));
            }
        }
        return Response.NULL;
    }

    private Slice adding() {
        Slice key = params().get(0);
        final RMZSet mapDBObj = getZSetFromBaseOrCreateEmpty(key);
        if (((params().size()) & 1) == 0) {
            throw new ArgumentException("ERR syntax error");
        }
        if (options.contains(XX) && params().isEmpty()) {
            return Slice.empty();
        }

        for (int i = 1; i < params().size(); i += 2) {
            Slice score = params().get(i);
            Slice value = params().get(i + 1);

            double newScore = toDouble(score.toString());

            addOneElement(mapDBObj, value, newScore);
        }
        if (countAdd + countChange > 0) {
            base().putValue(key, mapDBObj);
            lock.notifyAll();
        }
        return options.contains(CH) ? Response.integer(countAdd + countChange) :
                Response.integer(countAdd);
    }

    private void addOneElement(RMZSet mapDBObj, Slice value, double newScore) {
        if (options.contains(XX) && mapDBObj.hasMember(value)) {
            updateValue(mapDBObj, value, newScore);
        }
        if (options.contains(NX) && !mapDBObj.hasMember(value)) {
            mapDBObj.put(value, newScore);
            countAdd++;
        }
        if (!options.contains(XX) && !options.contains(NX)) {
            if (mapDBObj.hasMember(value)) {
                updateValue(mapDBObj, value, newScore);
            } else {
                mapDBObj.put(value, newScore);
                countAdd++;
            }
        }
    }

    @SuppressWarnings("UnnecessaryParentheses")
    private void updateValue(RMZSet mapDBObj, Slice value, double newScore) {
        Double oldScore = mapDBObj.getScore(value);
        if ((options.contains(LT) && oldScore > newScore)
                || (options.contains(GT) && oldScore < newScore)
                || (!options.contains(LT) && !options.contains(GT) && oldScore != newScore)) {
            mapDBObj.put(value, newScore);
            countChange++;
        }
    }

    private void parseParams() {
        Iterator<Slice> i = params().iterator();
        i.next();
        boolean quit = false;
        while (i.hasNext() && !quit) {
            String opt = i.next().toString();
            quit = true;
            for (Options value : Options.values()) {
                if (value.toString().equalsIgnoreCase(opt)) {
                    options.add(value);
                    i.remove();
                    quit = false;
                    break;
                }
            }
        }
    }

}
