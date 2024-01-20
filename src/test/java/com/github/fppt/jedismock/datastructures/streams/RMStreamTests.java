package com.github.fppt.jedismock.datastructures.streams;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.WrongStreamKeyException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;

import static java.lang.Long.toUnsignedString;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class RMStreamTests {
    RMStream stream;

    @BeforeEach
    void setup() {
        stream = new RMStream();
    }

    @Test
    void updateLastIdTest() {
        stream.updateLastId(new StreamId(1, 1));
        assertThat(stream.getLastId()).isEqualTo(new StreamId(1, 1));
    }

    @Test
    void whenLongMaxIsLastId_ensureThrowsExceptionOnAsteriskInput() {
        stream.updateLastId(new StreamId(-1, -1));
        assertThatThrownBy(
                () -> stream.replaceAsterisk(Slice.create("*"))
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR The stream has exhausted the last possible ID, unable to add more items");
    }

    @Test
    void whenLongMaxIsSecondPartLastId_ensureIncrementsCorrectlyOnAsteriskInput() {
        stream.updateLastId(new StreamId(3, -1));
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(Slice.create("*"))).isEqualTo(Slice.create("4-0"))
        ).doesNotThrowAnyException();
    }

    @Test
    void whenInvokeReplaceAsterisk_ensureWorksFineWithOnAsteriskInput() {
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(Slice.create("*"))).isEqualTo(Slice.create("0-1"))
        ).doesNotThrowAnyException();

        stream.updateLastId(new StreamId(1, 1));
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(Slice.create("*"))).isEqualTo(Slice.create("1-2"))
        ).doesNotThrowAnyException();
    }

    @Test
    void whenLongMaxIsSecondPartLastId_ensureThrowsExceptionOnNumberWithAsteriskInput() {
        stream.updateLastId(new StreamId(3, -1));
        assertThatThrownBy(
                () -> stream.replaceAsterisk(Slice.create("3-*"))
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    @Test
    void whenGivenNumberIsSmallerThanFirstPartLastId_ensureThrowsExceptionOnNumberWithAsteriskInput() {
        stream.updateLastId(new StreamId(1, 0));
        assertThatThrownBy(
                () -> stream.replaceAsterisk(Slice.create("0-*"))
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item");
    }

    @Test
    void whenNotANumberWasGivenWithAsterisk_ensureThrowsException() {
        assertThatThrownBy(
                () -> stream.replaceAsterisk(Slice.create("e-*"))
        )
                .isInstanceOf(WrongStreamKeyException.class)
                .hasMessage("ERR Invalid stream ID specified as stream command argument");
    }

    @Test
    void whenInvokeReplaceAsterisk_ensureWorksFineWithOnNumberWithAsteriskInput() {
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(Slice.create("1-*"))).isEqualTo(Slice.create("1-0"))
        ).doesNotThrowAnyException();

        stream.updateLastId(new StreamId(1, 3));
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(Slice.create("1-*"))).isEqualTo(Slice.create("1-4"))
        ).doesNotThrowAnyException();
    }

    @Test
    void whenKeyIsCorrect_ensureReplaceAsteriskDoesNotChangeIt() {
        Slice id = Slice.create("1-1");
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(id)).isSameAs(id) // Reference equality
        ).doesNotThrowAnyException();
    }

    @Test
    void whenKeyIsIncorrect_ensureReplaceAsteriskDoesNotChangeIt() {
        Slice id = Slice.create("qwerty");
        assertThatCode(
                () -> assertThat(stream.replaceAsterisk(id)).isSameAs(id) // Reference equality
        ).doesNotThrowAnyException();
    }

    @Test
    void stressAsteriskTest() {
        for (int i = 0; i < 1000; ++i) {
            if (Math.random() >= 0.9) {
                long second = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);
                long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);

                stream.updateLastId(new StreamId(first, second));

                assertThatCode(
                        () -> assertThat(
                                stream.replaceAsterisk(Slice.create(toUnsignedString(first) + "-*"))
                        ).isEqualTo(
                                Slice.create(toUnsignedString(first) + "-" + toUnsignedString(second + 1))
                        )
                ).doesNotThrowAnyException();

                assertThatCode(
                        () -> assertThat(
                                stream.replaceAsterisk(Slice.create("*"))
                        ).isEqualTo(
                                Slice.create(toUnsignedString(first) + "-" + toUnsignedString(second + 1))
                        )
                ).doesNotThrowAnyException();
            } else {
                long second = -1;
                long first = (long) (Math.random() * Long.MAX_VALUE) * (Math.random() >= 0.5 ? 1 : -1);

                stream.updateLastId(new StreamId(first, second));

                assertThatThrownBy(
                        () -> stream.replaceAsterisk(Slice.create(toUnsignedString(first) + "-*"))
                )
                        .isInstanceOf(WrongStreamKeyException.class)
                        .hasMessage("ERR The ID specified in XADD is equal or smaller than the target stream top item");

                assertThatCode(
                        () -> assertThat(
                                stream.replaceAsterisk(Slice.create("*"))
                        ).isEqualTo(
                                Slice.create(toUnsignedString(first + 1) + "-0")
                        )
                ).doesNotThrowAnyException();
            }
        }
    }
}
