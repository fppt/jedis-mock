package com.github.fppt.jedismock.operations.keys.paramsparser;

import com.github.fppt.jedismock.datastructures.Slice;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static java.util.stream.Collectors.toList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpirationExtraParamTest {
    private List<Slice> slices(String... strings) {
        return Arrays.stream(strings).map(Slice::create).collect(toList());
    }

    @Test
    void unsupportedOption() {
        assertThatThrownBy(() ->
                new ExpirationExtraParam(slices(
                        "_", "_", "NX", "??"
                ), false
                )
        ).isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("Unsupported option ??");
    }

    @Test
    void usupportedOptionExpected() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "XX", "GT", "??"), true
        );
        assertThat(param.getIndex()).isEqualTo(4);
    }

    @Test
    void validCombinationNX() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "NX"
        ), true
        );
        assertThat(param.getIndex()).isEqualTo(3);
    }

    @Test
    void invalidCombinationNX() {
        assertThatThrownBy(() ->
                new ExpirationExtraParam(slices(
                        "_", "_", "NX", "GT"
                ), false
                )
        ).isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("not compatible");
    }

    @Test
    void validCombinationGT() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "GT"
        ), true
        );
        assertThat(param.getIndex()).isEqualTo(3);
    }

    @Test
    void invalidCombinationGTLT() {
        assertThatThrownBy(() ->
                new ExpirationExtraParam(slices(
                        "_", "_", "XX", "GT", "LT"
                ), false
                )
        ).isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("not compatible");
    }

    @Test
    void checkTimingNonExistentKey() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_"
        ), false);
        assertThat(param.checkTiming(false, 0L, 0L)).isFalse();
    }

    @Test
    void checkTimingNXTrue() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "NX"
        ), false);
        assertThat(param.checkTiming(true, null, 10L)).isTrue();
    }

    @Test
    void checkTimingNXFalse() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "NX"
        ), false);
        assertThat(param.checkTiming(true, 10L, 100L)).isFalse();
    }

    @Test
    void checkTimingXXTrue() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "XX"
        ), false);
        assertThat(param.checkTiming(true, 10L, 11L)).isTrue();
    }

    @Test
    void checkTimingXXFalse() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "XX"
        ), false);
        assertThat(param.checkTiming(true, null, 100L)).isFalse();
    }

    @Test
    void checkTimingGTFalse() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "GT"
        ), false);
        assertThat(param.checkTiming(true, 100L, 10L)).isFalse();
    }

    @Test
    void checkTimingGTTrue() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "GT"
        ), false);
        assertThat(param.checkTiming(true, 10L, 100L)).isTrue();
    }

    @Test
    void checkTimingLTFalse() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "LT"
        ), false);
        assertThat(param.checkTiming(true, 10L, 100L)).isFalse();
    }

    @Test
    void checkTimingLTTrue() throws ExpirationParamsException {
        ExpirationExtraParam param = new ExpirationExtraParam(slices(
                "_", "_", "LT"
        ), false);
        assertThat(param.checkTiming(true, 100L, 10L)).isTrue();
    }
}