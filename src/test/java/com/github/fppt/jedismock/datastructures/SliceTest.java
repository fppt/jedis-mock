package com.github.fppt.jedismock.datastructures;

import nl.jqno.equalsverifier.EqualsVerifier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;

class SliceTest {
    @Test
    void equalsHashCode() {
        EqualsVerifier.forClass(Slice.class)
                .withNonnullFields("storedData")
                .verify();
    }

    @Test
    void empty() {
        Slice e1 = Slice.empty();
        Slice e2 = Slice.empty();
        assertThat(e1.length()).isZero();
        assertThat(e1).isSameAs(e2);
    }

    // --- 1) ASCII strings should compare the same as String#compareTo ---
    @ParameterizedTest(name = "\"{0}\" vs \"{1}\" → same as String.compareTo")
    @CsvSource({
            "'',''",          // equal empties
            "'','A'",         // empty < non-empty
            "'A','A'",
            "'A','B'",
            "'ABC','ABD'",
            "'abc','abc'",
            "'Z','a'",
            "'0','9'",
            "'Hello','Hello'",
            "'Hello','Hello!'",  // needs quotes due to punctuation
            "'Algo','Algorithm'"
    })
    void asciiOrderMatchesString(String left, String right) {
        Slice sLeft = Slice.create(left);
        Slice sRight = Slice.create(right);
        int expectedSign = Integer.signum(left.compareTo(right));
        int actualSign = Integer.signum(sLeft.compareTo(sRight));
        int actualNegSign = Integer.signum(sRight.compareTo(sLeft));
        //Check symmetry
        assertThat(-actualNegSign).isEqualTo(actualSign);
        assertThat(actualSign)
                .as("ASCII compare sign must match String.compareTo sign")
                .isEqualTo(expectedSign);
    }

    @ParameterizedTest(name = "unsigned lexicographic: [{0}] < [{1}]")
    @CsvSource({
            "'7F',   '80'",          // 0x7F (127) < 0x80 (128) in unsigned compare
            "'00',   'FF'",          // 0x00 < 0xFF
            "'80',   'FF'",          // 0x80 (128) < 0xFF (255)
            "'01 FF','02 00'",     // multi-byte: first differing byte decides
            "'01 22','01 FA'",     // multi-byte: first differing byte decides
            "'',     '00'",
            "'01',   '01 02'",
            "'01 02', '01 02 03'"
    })
    void unsignedByteLexicographic_leftIsLessThanRight(String leftHex, String rightHex) {
        byte[] left = bytes(leftHex);
        byte[] right = bytes(rightHex);

        Slice sLeft = Slice.create(left);
        Slice sRight = Slice.create(right);

        //Lengths
        assertThat(sLeft.length()).isEqualTo(left.length);
        assertThat(sRight.length()).isEqualTo(right.length);


        // left < right
        assertThat(sLeft.compareTo(sRight)).isLessThan(0);
        assertThat(sLeft).isLessThan(sRight);

        // symmetry: right > left
        assertThat(sRight.compareTo(sLeft)).isGreaterThan(0);
        assertThat(sRight).isGreaterThan(sLeft);
    }


    @ParameterizedTest(name = "equality: [{0}] == [{1}]")
    @CsvSource({
            "'',''",
            "'80','80'",
            "'01 02 03','01 02 03'"
    })
    void equality_compareToZeroAndEquals(String leftHex, String rightHex) {
        byte[] left = bytes(leftHex);
        byte[] right = bytes(rightHex);
        Slice sLeft = Slice.create(left);
        Slice sRight = Slice.create(right);
        assertThat(sLeft.compareTo(sRight)).isZero();
        assertThat(sRight.compareTo(sLeft)).isZero();
        assertThat(sLeft).isEqualByComparingTo(sRight);
        assertThat(sLeft).isEqualTo(sRight);
        assertThat(sLeft.hashCode()).isEqualTo(sRight.hashCode());
    }

    private static byte[] bytes(String hex) {
        String t = hex == null ? "" : hex.trim();
        if (t.isEmpty()) {
            return new byte[0];
        }
        String[] parts = t.split("\\s+");
        byte[] out = new byte[parts.length];
        for (int i = 0; i < parts.length; i++) {
            out[i] = (byte) Integer.parseInt(parts[i], 16);
        }
        return out;
    }
}
