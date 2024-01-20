package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XTrimParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.Collections;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(ComparisonBase.class)
public class XTrimTests {
    private static final Map<String, String> HASH = Collections.singletonMap("a", "b");
    
    @BeforeEach
    void setUp(Jedis jedis) {
        jedis.flushAll();
    }


    @TestTemplate
    void xtrimXdelAreReflectedByRecordedFirstEntry(Jedis jedis) {
        jedis.xadd("s", XAddParams.xAddParams().id("1-0"), HASH);
        jedis.xadd("s", XAddParams.xAddParams().id("2-0"), HASH);
        jedis.xadd("s", XAddParams.xAddParams().id("3-0"), HASH);
        jedis.xadd("s", XAddParams.xAddParams().id("4-0"), HASH);
        jedis.xadd("s", XAddParams.xAddParams().id("5-0"), HASH);

        assertThat(jedis.xlen("s")).isEqualTo(5);
        assertThat(jedis.xrange("s", StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, 1))
                .hasSize(1)
                .first()
                .usingRecursiveComparison()
                .isEqualTo(new StreamEntry(
                        new StreamEntryID(1, 0),
                        HASH
                ));

        jedis.xdel("s", new StreamEntryID(2, 0));

        assertThat(jedis.xrange("s", StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, 1))
                .hasSize(1)
                .first()
                .usingRecursiveComparison()
                .isEqualTo(new StreamEntry(
                        new StreamEntryID(1, 0),
                        HASH
                ));

        jedis.xdel("s", new StreamEntryID(1, 0));

        assertThat(jedis.xrange("s", StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, 1))
                .hasSize(1)
                .first()
                .usingRecursiveComparison()
                .isEqualTo(new StreamEntry(
                        new StreamEntryID(3, 0),
                        HASH
                ));

        jedis.xtrim("s", XTrimParams.xTrimParams().exactTrimming().maxLen(2));

        assertThat(jedis.xrange("s", StreamEntryID.MINIMUM_ID, StreamEntryID.MAXIMUM_ID, 1))
                .hasSize(1)
                .first()
                .usingRecursiveComparison()
                .isEqualTo(new StreamEntry(
                        new StreamEntryID(4, 0),
                        HASH
                ));
    }
}
