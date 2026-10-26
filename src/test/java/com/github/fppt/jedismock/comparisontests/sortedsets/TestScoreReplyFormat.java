package com.github.fppt.jedismock.comparisontests.sortedsets;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Checks the exact bytes Redis uses to render a score in a reply.
 * Jedis parses scores into doubles, so the regular comparison tests cannot see the difference
 * between e.g. {@code 0} and {@code -0}, {@code inf} and {@code Infinity}, or {@code 1e-7} and {@code 1.0E-7}.
 * These tests read the raw replies instead.
 */
@ExtendWith(ComparisonBase.class)
public class TestScoreReplyFormat {

    private static final String ZSET_KEY = "scores";

    private record Score(String member, String zaddArgument, String reply) {
        double value() {
            return Double.parseDouble(zaddArgument.replace("inf", "Infinity"));
        }
    }

    /**
     * One entry per formatting rule of real Redis: infinities, whole numbers within +/-2^62,
     * whole numbers beyond that (plain or exponent form), plain decimals and exponent form decimals.
     */
    private static final List<Score> SCORES = List.of(
            new Score("pinf", "+inf", "inf"),
            new Score("ninf", "-inf", "-inf"),
            new Score("whole", "3", "3"),
            new Score("negwhole", "-4", "-4"),
            new Score("negzero", "-0", "0"),
            new Score("maxint", "4611686018427387904", "4611686018427387904"),
            new Score("bigplain", "9223372036854775808", "9223372036854776000"),
            new Score("bigexp", "5e18", "5e+18"),
            new Score("hugeexp", "1e300", "1e+300"),
            new Score("frac", "1.5", "1.5"),
            new Score("bigfrac", "12345678.5", "12345678.5"),
            new Score("lt1", "0.25", "0.25"),
            new Score("small", "0.000015", "0.000015"),
            new Score("longfrac", "0.30000000000000004", "0.30000000000000004"),
            new Score("tiny", "1e-7", "1e-7"),
            new Score("tinyfrac", "1.5e-7", "1.5e-7"),
            new Score("negtiny", "-2.5e-9", "-2.5e-9"),
            new Score("tinyhuge", "1.5e-300", "1.5e-300"));

    private static void addAll(Jedis jedis) {
        for (Score score : SCORES) {
            jedis.sendCommand(Protocol.Command.ZADD, ZSET_KEY, score.zaddArgument(), score.member());
        }
    }

    private static List<String> strings(Object reply) {
        List<String> result = new ArrayList<>();
        for (Object element : (List<?>) reply) {
            result.add(element instanceof byte[] bytes ? new String(bytes, StandardCharsets.UTF_8) : element.toString());
        }
        return result;
    }

    private static List<String> membersWithScoresAscending() {
        List<String> result = new ArrayList<>();
        SCORES.stream().sorted(Comparator.comparingDouble(Score::value)).forEach(score -> {
            result.add(score.member());
            result.add(score.reply());
        });
        return result;
    }

    @TestTemplate
    public void zrangeWithScores(Jedis jedis) {
        addAll(jedis);
        assertThat(strings(jedis.sendCommand(Protocol.Command.ZRANGE, ZSET_KEY, "0", "-1", "WITHSCORES")))
                .containsExactlyElementsOf(membersWithScoresAscending());
    }

    @TestTemplate
    public void zmscore(Jedis jedis) {
        addAll(jedis);
        List<String> args = new ArrayList<>();
        args.add(ZSET_KEY);
        SCORES.forEach(score -> args.add(score.member()));
        assertThat(strings(jedis.sendCommand(Protocol.Command.ZMSCORE, args.toArray(new String[0]))))
                .containsExactlyElementsOf(SCORES.stream().map(Score::reply).toList());
    }

    @TestTemplate
    public void zrankWithScore(Jedis jedis) {
        addAll(jedis);
        for (Score score : SCORES) {
            assertThat(strings(jedis.sendCommand(Protocol.Command.ZRANK, ZSET_KEY, score.member(), "WITHSCORE")))
                    .element(1).as(score.member()).isEqualTo(score.reply());
            assertThat(strings(jedis.sendCommand(Protocol.Command.ZREVRANK, ZSET_KEY, score.member(), "WITHSCORE")))
                    .element(1).as(score.member()).isEqualTo(score.reply());
        }
    }

    @TestTemplate
    public void zpopmin(Jedis jedis) {
        addAll(jedis);
        assertThat(strings(jedis.sendCommand(Protocol.Command.ZPOPMIN, ZSET_KEY, String.valueOf(SCORES.size()))))
                .containsExactlyElementsOf(membersWithScoresAscending());
    }

    @TestTemplate
    public void zunionWithScores(Jedis jedis) {
        addAll(jedis);
        assertThat(strings(jedis.sendCommand(Protocol.Command.ZUNION, "1", ZSET_KEY, "WITHSCORES")))
                .containsExactlyElementsOf(membersWithScoresAscending());
    }
}
