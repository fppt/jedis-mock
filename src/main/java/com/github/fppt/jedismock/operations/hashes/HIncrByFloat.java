package com.github.fppt.jedismock.operations.hashes;

import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.KeyspaceEvent;
import com.github.fppt.jedismock.storage.RedisBase;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.List;
import java.util.Locale;

import static com.github.fppt.jedismock.Utils.convertToDouble;

@RedisCommand("hincrbyfloat")
class HIncrByFloat extends HIncrBy {
    HIncrByFloat(RedisBase base, List<Slice> params) {
        super(base, params);
    }

    private static void validateHIncrByFloatArgument(Slice input) {

        final String errorMessage = "ERR value is not a valid float";

        // validate input is not started/ended with spaces
        String foundValueStr = String.valueOf(input);
        if (foundValueStr.startsWith(" ") || foundValueStr.endsWith(" ")) {
            throw new IllegalArgumentException(errorMessage);
        }

        // validate input doesn't contain null-terminator symbols
        byte[] bts = input.data();
        for (byte bt : bts) {
            if (bt == 0) {
                throw new IllegalArgumentException(errorMessage);
            }
        }
    }

    @Override
    KeyspaceEvent incrementEvent() {
        return KeyspaceEvent.HINCRBYFLOAT;
    }

    @Override
    Slice hsetValue(Slice key1, Slice key2, Slice value) {
        double numericValue = convertToDouble(String.valueOf(value));
        Slice foundValue = base().getSlice(key1, key2);
        if (foundValue != null) {
            validateHIncrByFloatArgument(foundValue);
            numericValue = convertToDouble(new String(foundValue.data())) + numericValue;
        }
        // real redis returns 17 digits after dot
        DecimalFormatSymbols separator = new DecimalFormatSymbols(Locale.getDefault());
        separator.setDecimalSeparator('.');
        DecimalFormat formatter = new DecimalFormat("#.#################", separator);
        Slice res = Slice.create(formatter.format(numericValue));
        base().putSlice(key1, key2, res, -1L);
        return Response.bulkString(res);
    }

    //response() is inherited: it is identical to HIncrBy's and reports the
    //event returned by incrementEvent() above.
}
