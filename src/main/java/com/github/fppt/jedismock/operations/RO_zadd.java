package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.RMHMap;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.RedisBase;

import java.util.AbstractMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.github.fppt.jedismock.Utils.convertToDouble;
import static com.github.fppt.jedismock.Utils.serializeObject;
import static java.util.stream.Collectors.toMap;

@RedisCommand("zadd")
class RO_zadd extends AbstractRedisOperation {

    RO_zadd(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    @Override
    Slice response() {
        Slice key = params().get(0);

        final RMHMap mapDBObj = getHMapFromBase(key);
        final Map<Slice, Double> map = mapDBObj.getStoredData();

        int count = 0;
        for (int i = 1; i < params().size(); i += 2) {
            Slice score = params().get(i);
            Slice value = params().get(i + 1);

            // Score must be a double. Will throw an exception if it's not.
            double s = convertToDouble(score.toString());

            Double prevScore = map.put(value, s);
            if (prevScore == null) count++;
        }

        // Sort the map by value (the score) before saving
        Map<Slice, Double> sortedMap = map.entrySet()
            .stream()
            .map(x -> new AbstractMap.SimpleEntry<>(x.getKey(), convertToDouble(x.getValue().toString())))
            .sorted(Map.Entry.comparingByValue())
            .collect(
                toMap(Map.Entry::getKey, Map.Entry::getValue, (e1, e2) -> e2, LinkedHashMap::new));

        try {
            base().putSlice(key, serializeObject(sortedMap));
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage());
        }
        return Response.integer(count);
    }

}
