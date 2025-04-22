package com.github.fppt.jedismock.operations.keys.paramsparser;

import com.github.fppt.jedismock.datastructures.Slice;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static com.github.fppt.jedismock.operations.keys.paramsparser.ExpirationExtraParamTest.slices;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class ExpirationFieldsParamTest {
    @Test
    void testExpirationFieldsParam() throws ExpirationParamsException {
        ExpirationFieldsParam param = new ExpirationFieldsParam(
                slices("_", "_", "_", "FIELDS", "2", "a", "b"),
                3
        );
        assertThat(param.getFields().stream().map(Slice::toString)).containsExactly("a", "b");
    }

    @Test
    void testExpirationFieldsParamNoFields() {
        assertThatThrownBy(() -> new ExpirationFieldsParam(slices("_", "_", "foo", "2", "a", "b"),
                2)).isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("FIELDS is missing");
    }

    @CsvSource({"0", "-1", "foo"})
    @ParameterizedTest
    void testExpirationFieldsParamInvalidNumberOfFields(String invalidNumber) {
        assertThatThrownBy(() -> new ExpirationFieldsParam(slices("_", "_", "fields", invalidNumber, "a", "b"),
                2)).isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("numFields");
    }

    @Test
    void testFildsNumberMismatch() {
        assertThatThrownBy(() -> new ExpirationFieldsParam(slices("_", "_", "fields", "3", "a", "b"),
                2)).isInstanceOf(ExpirationParamsException.class)
                .hasMessageContaining("must match");
    }

}