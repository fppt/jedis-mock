package com.github.fppt.jedismock.operations.sortedsets;

import com.github.fppt.jedismock.datastructures.RMZSet;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.datastructures.ZSetEntry;
import com.github.fppt.jedismock.exception.ArgumentException;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.Stream;

abstract class ZStore extends AbstractByScoreOperation {

    protected static final String IS_WEIGHTS = "WEIGHTS";
    protected static final String IS_AGGREGATE = "AGGREGATE";
    protected static final String IS_WITHSCORES = "WITHSCORES";
    protected static final String IS_LIMIT = "LIMIT";

    protected boolean isLimit = false;
    protected long limit = 0;
    protected int startKeysIndex = 0;
    protected ArrayList<Double> weights;

    protected BiFunction<Double, Double, Double> aggregate = getSum();

    protected boolean withScores = false;

    ZStore(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    abstract protected RMZSet getResult(RMZSet zset1, RMZSet zset2, double weight);

    protected RMZSet getFinishedZSet() {
        int numKeys = Integer.parseInt(params().get(startKeysIndex).toString());
        if (numKeys == 0) {
            throw new ArgumentException("*at least 1 input key * '" + this.getClass().getSimpleName().toLowerCase() + "' command");
        }
        parseParams(numKeys);
        if (params().size() != numKeys + startKeysIndex + 1) {
            throw new ArgumentException("ERR syntax error*");
        }
        RMZSet mapDBObj = new RMZSet();
        RMZSet temp = getZSet(params().get(startKeysIndex + 1));
        for (ZSetEntry entry :
                temp.entries(false)) {
            mapDBObj.put(entry.getValue(), getMultiple(entry.getScore(), weights.get(0)));
        }
        for (int i = 1; i < numKeys; i++) {
           mapDBObj = getResult(mapDBObj, getZSet(params().get(startKeysIndex + i + 1)), weights.get(i));
        }

        return mapDBObj;
    }

    private RMZSet getZSet(Slice setName) {
        if (base().exists(setName)) {
            String typeName = base().getValue(setName).getTypeName();
            if ("zset".equalsIgnoreCase(typeName)) {
                return base().getZSet(setName);
            }
            if ("set".equalsIgnoreCase(typeName)) {
                RMZSet result = new RMZSet();
                for (Slice value : base().getSet(setName).getStoredData()) {
                    result.put(value, 1);
                }
                return result;
            }
        }
        return new RMZSet();
    }

   private void parseParams(int numKeys) {
       weights = new ArrayList<>(numKeys);
       for (int i = 0; i < numKeys; i++) {
           weights.add(1.0);
       }

       List<Slice> temp = new ArrayList<>(params());
       for (Slice param : temp) {
           if (IS_WEIGHTS.equalsIgnoreCase(param.toString())) {
               int index = params().indexOf(param);
               for (int i = 0; i < numKeys; i++) {
                   double weight;
                   try {
                       weight = toDouble(params().get(index + 1).toString());
                   } catch (IndexOutOfBoundsException e) {
                       throw new ArgumentException("ERR syntax error*");
                   }
                   weights.set(i, weight);
                   params().remove(index + 1);
               }
               params().remove(param);
           }
           if (IS_AGGREGATE.equalsIgnoreCase(param.toString())) {
               String aggParam;
               try {
                   aggParam = params().get(params().indexOf(param) + 1).toString();
               } catch (IndexOutOfBoundsException e) {
                   throw new ArgumentException("ERR syntax error*");
               }
               if ("MIN".equalsIgnoreCase(aggParam)) {
                   aggregate = Double::min;
               }
               if ("MAX".equalsIgnoreCase(aggParam)) {
                   aggregate = Double::max;
               }
               if ("SUM".equalsIgnoreCase(aggParam)) {
                   aggregate = getSum();
               }
               params().remove(params().indexOf(param) + 1);
               params().remove(param);
           }
           if (IS_WITHSCORES.equalsIgnoreCase(param.toString())) {
               withScores = true;
               if (this.getClass().getSimpleName().endsWith("Store")) {
                   throw new ArgumentException("ERR syntax error");
               }
               params().remove(param);
           }
           if (IS_LIMIT.equalsIgnoreCase(param.toString())) {
               isLimit = true;
               try {
                   limit = Long.parseLong(params().get(params().indexOf(param) + 1).toString());
               } catch (IndexOutOfBoundsException e) {
                   throw new ArgumentException("ERR syntax error*");
               } catch (NumberFormatException e) {
                   throw new ArgumentException("ERR LIMIT*");
               }
               if (limit < 0) {
                   throw new ArgumentException("ERR LIMIT* Negative limit");
               }
               params().remove(params().indexOf(param) + 1);
               params().remove(param);
           }
       }
   }

    private static BiFunction<Double, Double, Double> getSum() {
        return (a, b) -> {
            if (a.isInfinite() && b.isInfinite()) {
                if (a.equals(b)) {
                    return a;
                } else {
                    return Double.valueOf(0);
                }
            }
            return a + b;
        };
    }

    protected static Double getMultiple(Double score, Double weight) {
        if (score == 0 || weight == 0) {
            return 0.;
        }
        return score * weight;
    }

    protected long getResultSize() {
        Slice keyDest = params().get(0);
        if (base().exists(keyDest)) {
            base().deleteValue(keyDest);
        }
        startKeysIndex = 1;
        RMZSet mapDBObj = getFinishedZSet();
        if (!mapDBObj.isEmpty()) {
            base().putValue(keyDest, mapDBObj);
        }
        return mapDBObj.size();
    }

    protected List<Slice> getResultArray() {
        startKeysIndex = 0;
        return getFinishedZSet().entries(false).stream()
                .flatMap(e -> withScores
                        ? Stream.of(e.getValue(),
                        Slice.create(String.valueOf(Math.round(e.getScore()))))
                        : Stream.of(e.getValue()))
                .map(Response::bulkString)
                .collect(Collectors.toList());
    }

}
