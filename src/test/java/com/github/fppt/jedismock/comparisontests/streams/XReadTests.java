package com.github.fppt.jedismock.comparisontests.streams;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.StreamEntryID;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.jedis.params.XAddParams;
import redis.clients.jedis.params.XReadParams;
import redis.clients.jedis.resps.StreamEntry;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class XReadTests {
    private ExecutorService blockingThread;
    private Jedis blockedClient;

    @BeforeEach
    public void setUp(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        blockingThread = Executors.newSingleThreadExecutor();
    }

    @AfterEach
    public void tearDown() {
        blockedClient.close();
        blockingThread.shutdownNow();
    }

    @TestTemplate
    void blockingXREADforStreamThatRanDry(Jedis jedis) throws ExecutionException, InterruptedException {
        jedis.xadd("s", XAddParams.xAddParams().id("666"), Collections.singletonMap("a", "b"));
        jedis.xdel("s", new StreamEntryID(666));

        jedis.xread(XReadParams.xReadParams().block(10), Collections.singletonMap("s", new StreamEntryID(665)));

        assertThatThrownBy(
                () -> jedis.xadd("s", XAddParams.xAddParams().id("665"), Collections.singletonMap("a", "b"))
        )
                .isInstanceOf(JedisDataException.class)
                .hasMessageMatching("ERR.*equal.*smaller.*");

        Future<?> future = blockingThread.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> data = blockedClient.xread(
                    XReadParams.xReadParams().block(0),
                    Collections.singletonMap("s", new StreamEntryID(665))
            );

            assertThat(data)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asList()
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(667),
                                    Collections.singletonMap("a", "b")
                            )
                    );
        });

        jedis.xadd("s", XAddParams.xAddParams().id("667"), Collections.singletonMap("a", "b"));

        future.get();
    }
}
