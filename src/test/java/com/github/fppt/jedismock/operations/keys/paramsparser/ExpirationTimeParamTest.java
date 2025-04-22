package com.github.fppt.jedismock.operations.keys.paramsparser;

import com.github.fppt.jedismock.datastructures.Slice;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpirationTimeParamTest {
    @Test
    void normalSeconds() throws ExpirationParamsException {
        ExpirationTimeParam param = new ExpirationTimeParam(null,
                Slice.create("42"), false, 0);
        assertThat(param.getMillis()).isEqualTo(42000);
    }

    @Test
    void normalMillis() throws ExpirationParamsException {
        ExpirationTimeParam param = new ExpirationTimeParam(null,
                Slice.create("42"), true, 0);
        assertThat(param.getMillis()).isEqualTo(42);
    }

    @Test
    void notAnInteger() {
        assertThatThrownBy(() -> new ExpirationTimeParam
                (null, Slice.create("foo"), false, 0))
                .isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("not an integer");
    }

    @Test
    void overflow() {
        assertThatThrownBy(() -> new ExpirationTimeParam
                ("cmd", Slice.create(Long.toString(Long.MAX_VALUE)), false, 10))
                .isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("cmd")
                .hasMessageContaining("invalid expire time");

    }
}