package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.BlockingManager;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.RedisConfiguration;
import com.github.fppt.jedismock.storage.ScriptingManager;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;

import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Clock;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class RedisServer {

    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisServer.class);
    private static final String NOT_RUNNING = "JedisMock is not running";
    /**
     * How often {@link #stop()} warns while still waiting for client tasks to
     * terminate. The wait itself is unbounded: only a stuck client task (e.g.
     * in a user interceptor) can prolong it.
     */
    private static final long CLIENT_TERMINATION_WARN_INTERVAL_MILLIS = 10_000L;

    private final int bindPort;
    private final InetAddress bindAddress;
    private final Map<Integer, RedisBase> redisBases;
    private final BlockingManager blockingManager = new BlockingManager();
    private final ScriptingManager scriptingManager = new ScriptingManager();
    private final RedisConfiguration configuration = new RedisConfiguration();
    private final SubscriptionRegistry subscriptionRegistry = new SubscriptionRegistry();
    private final AtomicLong clientIds = new AtomicLong();
    /**
     * Serializes {@link #start()}, {@link #stop()} and SHUTDOWN-requested stops.
     * Never held while serving a command.
     */
    private final Object lifecycle = new Object();
    private volatile ExecutorService singleThreadPool;
    private volatile RedisServiceJob service;
    private volatile Clock clock = Clock.systemDefaultZone();
    private volatile ServiceOptions options = ServiceOptions.defaultOptions();
    private volatile Future<Void> serviceFinalization;

    public RedisServer() {
        this(0);
    }


    public RedisServer(int port) {
        this(port, null);
    }

    public RedisServer(int port, InetAddress address) {
        this.bindPort = port;
        this.bindAddress = address;
        this.redisBases = new HashMap<>();
        CommandFactory.initialize();
    }

    public static RedisServer newRedisServer() {
        return new RedisServer();
    }

    public static RedisServer newRedisServer(int port) {
        return new RedisServer(port);
    }

    public static RedisServer newRedisServer(int port, InetAddress address) {
        return new RedisServer(port, address);
    }

    public RedisServer setOptions(ServiceOptions options) {
        this.options = Objects.requireNonNull(options);
        return this;
    }

    public RedisServer setClock(Clock clock) {
        this.clock = Objects.requireNonNull(clock);
        return this;
    }

    public RedisServer start() throws IOException {
        rejectIfInsideCommandHandler("start");
        synchronized (lifecycle) {
            if (!(service == null)) {
                throw new IllegalStateException();
            }
            scriptingManager.endShutdown();
            this.service = new RedisServiceJob();
            singleThreadPool = Executors.newSingleThreadExecutor();
            serviceFinalization = singleThreadPool.submit(service);
            return this;
        }
    }

    public void stop() throws IOException {
        rejectIfInsideCommandHandler("stop");
        synchronized (lifecycle) {
            doStop(Objects.requireNonNull(service, NOT_RUNNING));
        }
    }

    /**
     * {@link #stop()} waits for all client tasks, so a client task calling it
     * (e.g. a command interceptor) would wait for itself. Use
     * {@link RedisClient#shutdownServer()} from inside a command handler.
     */
    private void rejectIfInsideCommandHandler(String method) {
        if (RedisClient.isServing(this)) {
            throw new IllegalStateException(method + "() must not be called from a command handler of this server,"
                    + " which would wait for itself: use RedisClient.shutdownServer() (state.owner().shutdownServer())"
                    + " to stop it from a separate thread instead");
        }
    }

    /**
     * Stop, as requested by {@code SHUTDOWN} on a connection of the given
     * generation. Ignored if that generation is no longer the running one
     * (e.g. a concurrent SHUTDOWN already stopped it, and it may have been
     * restarted since).
     */
    void stopRequestedBy(Object generation) throws IOException {
        synchronized (lifecycle) {
            if (service == null || service != generation) {
                return;
            }
            doStop(service);
        }
    }

    /**
     * @return the current generation of the server (a new one per
     * {@link #start()}), {@code null} while stopped.
     */
    Object currentGeneration() {
        return service;
    }

    private void doStop(RedisServiceJob job) throws IOException {
        //Locals: once service is null, a concurrent start() may replace these fields.
        Future<Void> acceptLoop = serviceFinalization;
        ExecutorService acceptor = singleThreadPool;
        //The accept loop registers a connection only after accept() returns,
        //so it must be over before the set of clients is taken as final.
        job.closeListener();
        awaitUninterruptibly(acceptLoop);
        job.closeClients();
        //The stopped server must be quiescent (manager and data are shared by
        //every start of this instance): kill the script, and any script that
        //starts before the next start(); drain the data lock, which also lets
        //queued commands of the closed clients be dropped (see MockExecutor);
        //then, without holding that lock, wait for the client tasks to end.
        scriptingManager.beginShutdown();
        synchronized (redisBases) {
            LOG.debug("Jedis-mock data lock drained for stop");
        }
        job.awaitClientsTerminated();
        service = null;
        acceptor.shutdownNow();
    }

    /**
     * Wait for the accept loop to end, uninterruptibly (the interrupt is
     * restored afterwards). It ends with an exception once the listener is
     * closed, which is normal.
     */
    private static void awaitUninterruptibly(Future<Void> acceptLoop) {
        boolean interrupted = false;
        try {
            while (true) {
                try {
                    acceptLoop.get();
                    return;
                } catch (ExecutionException e) {
                    //The listener was closed under accept(): the loop is over.
                    return;
                } catch (InterruptedException e) {
                    interrupted = true;
                }
            }
        } finally {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    public boolean isRunning() {
        return service != null;
    }

    public String getHost() {
        Objects.requireNonNull(service, NOT_RUNNING);

        InetAddress addr = service.getServer().getInetAddress();

        if (addr.isAnyLocalAddress()) {
            return addr instanceof Inet6Address ? "::1" : "127.0.0.1";
        }

        return addr.getHostAddress();
    }

    public int getBindPort() {
        Objects.requireNonNull(service, NOT_RUNNING);
        return service.getServer().getLocalPort();
    }

    Map<Integer, RedisBase> getRedisBases() {
        return redisBases;
    }

    BlockingManager getBlockingManager() {
        return blockingManager;
    }

    ScriptingManager getScriptingManager() {
        return scriptingManager;
    }

    RedisConfiguration getConfiguration() {
        return configuration;
    }

    SubscriptionRegistry getSubscriptionRegistry() {
        return subscriptionRegistry;
    }

    /**
     * Ids handed out to connections, mirroring the monotonically increasing,
     * server-wide client ids of a real Redis (reported by HELLO).
     *
     * @return the id for the next connection.
     */
    long nextClientId() {
        return clientIds.incrementAndGet();
    }

    public ServiceOptions options() {
        return options;
    }

    public Clock getClock() {
        return clock;
    }

    private final class RedisServiceJob implements Callable<Void> {

        private final ServerSocket server;
        private final ExecutorService threadPool = Executors.newCachedThreadPool();
        private final List<RedisClient> clients = new CopyOnWriteArrayList<>();

        public RedisServiceJob() throws IOException {
            Objects.requireNonNull(redisBases);
            Objects.requireNonNull(options);
            this.server = new ServerSocket(bindPort, 0, bindAddress);
        }

        public Void call() throws IOException {
            while (!server.isClosed()) {
                Socket socket = server.accept();
                RedisClient rc = new RedisClient(RedisServer.this, socket, clients::remove, this);
                clients.add(rc);
                threadPool.submit(rc);
            }
            return null;
        }

        public ServerSocket getServer() {
            return server;
        }

        /**
         * Release the port. Done first, so that a client whose connection is
         * dropped by SHUTDOWN cannot reconnect in between.
         */
        public void closeListener() throws IOException {
            server.close();
        }

        /**
         * Close every connection and interrupt their tasks. Only valid once
         * the accept loop has ended, so that no connection is registered or
         * submitted after this.
         */
        public void closeClients() {
            clients.forEach(RedisClient::close);
            threadPool.shutdownNow();
        }

        /**
         * Wait, uninterruptibly and without a limit, until every client task
         * has terminated, warning every
         * {@link #CLIENT_TERMINATION_WARN_INTERVAL_MILLIS} meanwhile. The
         * server counts as stopped only once this returns.
         */
        public void awaitClientsTerminated() {
            long waitedMillis = 0L;
            boolean interrupted = false;
            boolean terminated = false;
            try {
                while (!terminated) {
                    try {
                        terminated = threadPool.awaitTermination(
                                CLIENT_TERMINATION_WARN_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
                        if (!terminated) {
                            waitedMillis += CLIENT_TERMINATION_WARN_INTERVAL_MILLIS;
                            LOG.warn("Jedis-mock stop is still waiting for a client task to terminate after {} ms",
                                    waitedMillis);
                        }
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

}
