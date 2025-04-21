package com.github.fppt.jedismock.operations.keys.paramsparser;

import com.github.fppt.jedismock.datastructures.Slice;

import java.util.Collections;
import java.util.List;

public final class ExpirationFieldsParam {
    private List<Slice> fields;

    public ExpirationFieldsParam(List<Slice> params, int position) throws ExpirationParamsException {
        if (!"FIELDS".equalsIgnoreCase(params.get(position).toString())) {
            throw new ExpirationParamsException("ERR Mandatory argument FIELDS is missing " +
                    "or not at the right position");
        }
        long numFields = 0;
        try {
            numFields = Long.parseLong(new String(params.get(position + 1).data()));
        } catch (NumberFormatException e) {
            // do nothing. Even when the number format is wrong, "real" Redis says
            // "numFields should be > 0".
        }
        if (numFields <= 0) {
            throw new ExpirationParamsException("ERR Parameter `numFields` should be greater than 0");
        }
        if (params.size() - position - 2 != numFields) {
            throw new ExpirationParamsException("ERR The `numfields` parameter must match the number of arguments");
        }
        fields = Collections.unmodifiableList(params.subList(position + 2, params.size()));
    }

    public List<Slice> getFields() {
        return fields;
    }

}
