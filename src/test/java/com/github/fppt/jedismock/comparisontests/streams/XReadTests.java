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

import org.testcontainers.shaded.org.awaitility.Awaitility;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;

@ExtendWith(ComparisonBase.class)
public class XReadTests {
    private static final Pattern BLOCKED_CLIENTS = Pattern.compile("blocked_clients:(\\d+)");

    private ScheduledExecutorService scheduledThreadPool;
    private Jedis blockedClient;

    @BeforeEach
    public void setUp(Jedis jedis, HostAndPort hostAndPort) {
        jedis.flushAll();
        blockedClient = new Jedis(hostAndPort.getHost(), hostAndPort.getPort());
        scheduledThreadPool = Executors.newScheduledThreadPool(4);
    }

    @AfterEach
    public void tearDown() {
        blockedClient.close();
        scheduledThreadPool.shutdownNow();
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

        Future<?> future = scheduledThreadPool.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> data = blockedClient.xread(
                    XReadParams.xReadParams().block(0),
                    Collections.singletonMap("s", new StreamEntryID(665))
            );

            assertThat(data)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asInstanceOf(LIST)
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

    @TestTemplate
    void whenAddIsInvokedWithSingleStream_EnsureTemporaryBlockedXreadIsAwaken(Jedis jedis)
            throws ExecutionException, InterruptedException {
        Future<?> blockingReadJob = scheduledThreadPool.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> answer = blockedClient.xread(
                    XReadParams.xReadParams().block(20_000),
                    Collections.singletonMap("myst", new StreamEntryID(0, 7))
            );

            assertThat(answer)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asInstanceOf(LIST)
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(0, 8),
                                    Collections.singletonMap("a", "b")
                            )
                    );
        });

        ScheduledFuture<?> addJob = scheduledThreadPool.schedule(() -> {
            jedis.xadd(
                    "myst",
                    XAddParams.xAddParams().id(0, 8),
                    Collections.singletonMap("a", "b")
            );
        }, 2, TimeUnit.SECONDS);


        ScheduledFuture<?> cancellationJob = scheduledThreadPool.schedule(() -> {
            blockingReadJob.cancel(true);
        }, 15, TimeUnit.SECONDS);

        addJob.get();
        cancellationJob.get();
        blockingReadJob.get();

        assertThat(blockingReadJob.isCancelled()).isFalse();
    }

    @TestTemplate
    void whenAddIsInvokedWithSeveralStreams_EnsureTemporaryBlockedXreadAwakenImmediately(Jedis jedis)
            throws ExecutionException, InterruptedException {
        Future<?> blockingReadJob = scheduledThreadPool.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> answer = blockedClient.xread(
                    XReadParams.xReadParams().block(20_000),
                     new HashMap<String, StreamEntryID>() {{
                         put("fst", new StreamEntryID(0, 7));
                         put("and", new StreamEntryID(1, 2));
                    }}
            );

            assertThat(answer)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asInstanceOf(LIST)
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(0, 8),
                                    Collections.singletonMap("a", "b")
                            )
                    );
        });

        ScheduledFuture<?> fstAddJob = scheduledThreadPool.schedule(() -> {
            jedis.xadd(
                    "fst",
                    XAddParams.xAddParams().id(0, 8),
                    Collections.singletonMap("a", "b")
            );
        }, 2, TimeUnit.SECONDS);

        ScheduledFuture<?> sndAddJob = scheduledThreadPool.schedule(() -> {
            jedis.xadd(
                    "snd",
                    XAddParams.xAddParams().id(1, 3),
                    Collections.singletonMap("a", "b")
            );
        }, 8, TimeUnit.SECONDS);


        ScheduledFuture<?> cancellationJob = scheduledThreadPool.schedule(() -> {
            blockingReadJob.cancel(true);
        }, 15, TimeUnit.SECONDS);

        fstAddJob.get();
        sndAddJob.get();
        cancellationJob.get();
        blockingReadJob.get();

        assertThat(blockingReadJob.isCancelled()).isFalse();
    }

    /**
     * Mirrors the tcl stream suite, which calls {@code wait_for_blocked_client}
     * (polling INFO's {@code blocked_clients}) before XADD. A blocked XREAD must
     * be reflected in that count, and must drop back to zero once served.
     */
    @TestTemplate
    void blockedXreadIsReportedInInfoBlockedClients(Jedis jedis)
            throws ExecutionException, InterruptedException {
        assertThat(blockedClients(jedis)).isZero();

        Future<?> blockingReadJob = scheduledThreadPool.submit(() ->
                blockedClient.xread(
                        XReadParams.xReadParams().block(0),
                        Collections.singletonMap("st", new StreamEntryID(0, 7))
                ));

        Awaitility.await().until(() -> blockedClients(jedis) == 1);

        jedis.xadd("st", XAddParams.xAddParams().id(0, 8), Collections.singletonMap("a", "b"));

        blockingReadJob.get();
        Awaitility.await().until(() -> blockedClients(jedis) == 0);
    }

    private int blockedClients(Jedis jedis) {
        Matcher matcher = BLOCKED_CLIENTS.matcher(jedis.info("clients"));
        return matcher.find() ? Integer.parseInt(matcher.group(1)) : 0;
    }

    @TestTemplate
    void whenLastEntryIsCalledOnEmptyStream_EnsureXaddAwakes(Jedis jedis)
            throws ExecutionException, InterruptedException {
        Future<?> blockingReadJob = scheduledThreadPool.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> answer = blockedClient.xread(
                    XReadParams.xReadParams().block(0).count(1),
                    Collections.singletonMap("test:jedis", StreamEntryID.XGROUP_LAST_ENTRY)
            );

            assertThat(answer)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asInstanceOf(LIST)
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .comparingOnlyFields("fields")
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(0, 1),
                                    Collections.singletonMap("a", "b")
                            )
                    );
        });

        ScheduledFuture<?> addJob = scheduledThreadPool.schedule(() -> {
            jedis.xadd(
                    "test:jedis",
                    XAddParams.xAddParams(),
                    Collections.singletonMap("a", "b")
            );
        }, 2, TimeUnit.SECONDS);


        ScheduledFuture<?> cancellationJob = scheduledThreadPool.schedule(() -> {
            blockingReadJob.cancel(true);
        }, 15, TimeUnit.SECONDS);

        addJob.get();
        cancellationJob.get();
        blockingReadJob.get();

        assertThat(blockingReadJob.isCancelled()).isFalse();
    }

    /**
     * '$' must also work on a stream that exists but holds no entries (all
     * deleted): there is no "last entry" in it, so a timed blocking read that
     * nothing interrupts simply expires with an empty reply.
     */
    @TestTemplate
    void blockingXreadWithLastEntryOnStreamThatRanDryTimesOut(Jedis jedis) {
        jedis.xadd("dry", XAddParams.xAddParams().id(1, 1), Collections.singletonMap("a", "b"));
        jedis.xdel("dry", new StreamEntryID(1, 1));

        List<Map.Entry<String, List<StreamEntry>>> answer = jedis.xread(
                XReadParams.xReadParams().block(10),
                Collections.singletonMap("dry", StreamEntryID.XGROUP_LAST_ENTRY)
        );

        assertThat(answer).isNullOrEmpty();
    }

    /**
     * Same ran-dry stream, but with an infinite block: the reader must survive
     * until XADD delivers a new entry and then receive exactly that entry.
     */
    @TestTemplate
    void blockingXreadWithLastEntryOnStreamThatRanDryIsAwokenByXadd(Jedis jedis)
            throws ExecutionException, InterruptedException {
        jedis.xadd("dry", XAddParams.xAddParams().id(1, 1), Collections.singletonMap("a", "b"));
        jedis.xdel("dry", new StreamEntryID(1, 1));

        Future<?> blockingReadJob = scheduledThreadPool.submit(() -> {
            List<Map.Entry<String, List<StreamEntry>>> answer = blockedClient.xread(
                    XReadParams.xReadParams().block(0).count(1),
                    Collections.singletonMap("dry", StreamEntryID.XGROUP_LAST_ENTRY)
            );

            assertThat(answer)
                    .hasSize(1)
                    .first()
                    .extracting(Map.Entry::getValue)
                    .asInstanceOf(LIST)
                    .hasSize(1)
                    .first()
                    .usingRecursiveComparison()
                    .isEqualTo(
                            new StreamEntry(
                                    new StreamEntryID(5, 5),
                                    Collections.singletonMap("a", "b")
                            )
                    );
        });

        ScheduledFuture<?> addJob = scheduledThreadPool.schedule(() -> {
            jedis.xadd(
                    "dry",
                    XAddParams.xAddParams().id(5, 5),
                    Collections.singletonMap("a", "b")
            );
        }, 2, TimeUnit.SECONDS);

        ScheduledFuture<?> cancellationJob = scheduledThreadPool.schedule(() -> {
            blockingReadJob.cancel(true);
        }, 15, TimeUnit.SECONDS);

        addJob.get();
        cancellationJob.get();
        blockingReadJob.get();

        assertThat(blockingReadJob.isCancelled()).isFalse();
    }
}
