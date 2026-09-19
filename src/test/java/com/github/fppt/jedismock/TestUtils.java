package com.github.fppt.jedismock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestUtils {

    @Test
    public void testCloseQuietly() {
        Utils.closeQuietly(null);
        Utils.closeQuietly(new InputStream() {
            @Override
            public int read() {
                return 0;
            }

            @Override
            public void close() throws IOException {
                throw new IOException();
            }
        });
    }

    @Test
    public void parseQuotedStringReturnsEmptyStringForEmptyInput() {
        assertThat(Utils.parseQuotedString("")).isEqualTo("");
    }

    @Test
    public void parseQuotedStringReturnsUnquotedStringUnchanged() {
        assertThat(Utils.parseQuotedString("unquoted")).isEqualTo("unquoted");
    }

    @Test
    public void parseQuotedStringUnwrapsADoubleQuotedString() {
        assertThat(Utils.parseQuotedString("\"hello\"")).isEqualTo("hello");
    }

    @Test
    public void parseQuotedStringUnwrapsASingleQuotedString() {
        assertThat(Utils.parseQuotedString("'hello'")).isEqualTo("hello");
    }

    @Test
    public void parseQuotedStringUnwrapsAnEmptyQuotedString() {
        assertThat(Utils.parseQuotedString("\"\"")).isEqualTo("");
        assertThat(Utils.parseQuotedString("''")).isEqualTo("");
    }

    @Test
    public void parseQuotedStringPassesTheOtherQuoteCharacterThroughLiterally() {
        assertThat(Utils.parseQuotedString("\"foo'bar\"")).isEqualTo("foo'bar");
        assertThat(Utils.parseQuotedString("'foo\"bar'")).isEqualTo("foo\"bar");
    }

    @Test
    public void parseQuotedStringConcatenatesAnUnquotedPrefixWithAQuotedSuffix() {
        assertThat(Utils.parseQuotedString("pre\"quoted\"")).isEqualTo("prequoted");
    }

    @Test
    public void parseQuotedStringUnescapesAnEscapedQuoteCharacter() {
        assertThat(Utils.parseQuotedString("\"he said \\\"hi\\\"\"")).isEqualTo("he said \"hi\"");
        assertThat(Utils.parseQuotedString("'it\\'s'")).isEqualTo("it's");
    }

    @Test
    public void parseQuotedStringTreatsABackslashAsLiteralWhenNotFollowedByTheEnclosingQuote() {
        assertThat(Utils.parseQuotedString("\"a\\nb\"")).isEqualTo("a\\nb");
    }

    @Test
    public void parseQuotedStringRejectsAnUnterminatedQuote() {
        assertThat(Utils.parseQuotedString("\"foo")).isNull();
        assertThat(Utils.parseQuotedString("'foo")).isNull();
    }

    @Test
    public void parseQuotedStringRejectsATrailingBackslashWithNoClosingQuote() {
        assertThat(Utils.parseQuotedString("\"foo\\")).isNull();
    }

    @Test
    public void parseQuotedStringRejectsAnEscapedQuoteAsTheFinalCharacters() {
        assertThat(Utils.parseQuotedString("\"foo\\\"")).isNull();
    }

    @Test
    public void parseQuotedStringRejectsCharactersAfterTheClosingQuote() {
        assertThat(Utils.parseQuotedString("\"foo\"bar")).isNull();
        assertThat(Utils.parseQuotedString("\"foo\" ")).isNull();
    }
}
