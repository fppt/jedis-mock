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
     * How long {@link #stop()} waits for the last client task to terminate.
     * Only a client task that is stuck, which none should be, can exhaust it.
     */
    private static final long CLIENT_TERMINATION_TIMEOUT_MILLIS = 10_000L;

    private final int bindPort;
    private final InetAddress bindAddress;
    private final Map<Integer, RedisBase> redisBases;
    private final BlockingManager blockingManager = new BlockingManager();
    private final ScriptingManager scriptingManager = new ScriptingManager();
    private final RedisConfiguration configuration = new RedisConfiguration();
    private final SubscriptionRegistry subscriptionRegistry = new SubscriptionRegistry();
    private final AtomicLong clientIds = new AtomicLong();
    /**
     * Serializes the lifecycle transitions ({@link #start()}, {@link #stop()}
     * and the stop requested by a SHUTDOWN), so that two of them in flight at
     * once, say two SHUTDOWNs or a SHUTDOWN racing a restart, cannot act on
     * different generations of the server at the same time. Never held while
     * serving a command, so it cannot deadlock against client tasks.
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
        synchronized (lifecycle) {
            doStop(Objects.requireNonNull(service, NOT_RUNNING));
        }
    }

    /**
     * Stop, as requested by a {@code SHUTDOWN} received on a connection of the
     * given generation of the server. The request is stale, and ignored, when
     * that generation is no longer the running one: it has already been
     * stopped, say by a concurrent SHUTDOWN, and possibly restarted, and the
     * late request must neither arm the kill-on-start flag against the new
     * generation nor publish it as stopped. The generation, not the state of
     * the requesting connection, decides: a peer may hang up right after
     * sending SHUTDOWN, and that must not cancel the request.
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
     * @return an identity for the current generation of the server: changes
     * with every {@link #start()}, {@code null} while stopped.
     */
    Object currentGeneration() {
        return service;
    }

    private void doStop(RedisServiceJob job) throws IOException {
        //Work on locals: once the stopped state is published below, start() may
        //run (SHUTDOWN stops asynchronously, and its callers poll isRunning()
        //before restarting) and replace these fields with the new server's,
        //which this stop must neither wait on nor shut down.
        Future<Void> acceptLoop = serviceFinalization;
        ExecutorService acceptor = singleThreadPool;
        job.stop();
        //A stopped server must be quiescent: the scripting manager and the data
        //are shared by every start of this instance, so a script outliving the
        //stop would make a restarted server reply BUSY, and a command outliving
        //it could start one or, if it is a stale SHUTDOWN or KILL, act on the
        //restarted server. Kill the running script (its connection is already
        //closed, so the "killed" error is never sent), and arm the manager so
        //that any script starting from now on dies at its first instruction.
        //Then take the data lock once: that returns only after the script has
        //aborted and every command that was already waiting for the lock has
        //run its course (those of closed clients are dropped, see MockExecutor).
        //Finally wait for the client tasks themselves to terminate, so that not
        //even a task merely paused mid-dispatch outlives the server. That wait
        //must not hold the data lock: a task still queued on it can only find
        //out that its client is closed, and exit, once it gets the lock.
        //Neither wait can be cut short by an interrupt.
        scriptingManager.beginShutdown();
        synchronized (redisBases) {
            LOG.debug("Jedis-mock data lock drained for stop");
        }
        job.awaitClientsTerminated();
        service = null;
        try {
            acceptLoop.get();
        } catch (ExecutionException e) {
            //Do nothing: it's a normal behaviour when the service was stopped
        } catch (InterruptedException e) {
            LOG.warn("Jedis-mock interrupted while stopping");
            Thread.currentThread().interrupt();
        } finally {
            acceptor.shutdownNow();
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

        public void stop() throws IOException {
            //Release the port first: a client that notices its connection being
            //dropped (by SHUTDOWN) must not be able to reconnect in between.
            server.close();
            clients.forEach(RedisClient::close);
            threadPool.shutdownNow();
        }

        /**
         * Wait, without being interruptible, until every client task has
         * terminated: their sockets are closed and their threads interrupted,
         * so the only ones left are mid-command, and those finish on their own
         * (a script dies at its next instruction once the manager is armed, a
         * command waiting for the data lock is dropped as soon as it gets it).
         * Gives up, with a warning, after {@link #CLIENT_TERMINATION_TIMEOUT_MILLIS}.
         */
        public void awaitClientsTerminated() {
            long deadline = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(CLIENT_TERMINATION_TIMEOUT_MILLIS);
            boolean interrupted = false;
            boolean terminated = false;
            try {
                while (!terminated) {
                    long remaining = deadline - System.nanoTime();
                    if (remaining <= 0) {
                        LOG.warn("Jedis-mock stopped with a client task still running after {} ms",
                                CLIENT_TERMINATION_TIMEOUT_MILLIS);
                        return;
                    }
                    try {
                        terminated = threadPool.awaitTermination(remaining, TimeUnit.NANOSECONDS);
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
