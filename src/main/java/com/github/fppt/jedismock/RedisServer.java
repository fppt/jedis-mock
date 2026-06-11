package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.BlockingManager;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.RedisConfiguration;
import com.github.fppt.jedismock.storage.ScriptingManager;
import org.jspecify.annotations.Nullable;

import java.io.IOException;
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

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class RedisServer {

    private static final String NOT_RUNNING = "JedisMock is not running";

    private final int bindPort;
    private final @Nullable InetAddress bindAddress;
    private final Map<Integer, RedisBase> redisBases;
    private final BlockingManager blockingManager = new BlockingManager();
    private final ScriptingManager scriptingManager = new ScriptingManager();
    private final RedisConfiguration configuration = new RedisConfiguration();
    private volatile @Nullable ExecutorService singleThreadPool;
    private volatile @Nullable RedisServiceJob service;
    private volatile Clock clock = Clock.systemDefaultZone();
    private volatile ServiceOptions options = ServiceOptions.defaultOptions();
    private volatile @Nullable Future<Void> serviceFinalization;

    public RedisServer() {
        this(0);
    }


    public RedisServer(int port) {
        this(port, null);
    }

    public RedisServer(int port, @Nullable InetAddress address) {
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

    public static RedisServer newRedisServer(int port, @Nullable InetAddress address) {
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
        if (!(service == null)) {
            throw new IllegalStateException();
        }
        this.service = new RedisServiceJob();
        singleThreadPool = Executors.newSingleThreadExecutor();
        serviceFinalization = singleThreadPool.submit(service);
        return this;
    }

    public void stop() throws IOException {
        RedisServiceJob runningService = service;
        Future<Void> finalization = serviceFinalization;
        ExecutorService pool = singleThreadPool;
        if (runningService == null || finalization == null || pool == null) {
            throw new NullPointerException(NOT_RUNNING);
        }
        runningService.stop();
        service = null;
        try {
            finalization.get();
        } catch (ExecutionException e) {
            //Do nothing: it's a normal behaviour when the service was stopped
        } catch (InterruptedException e) {
            System.err.println("Jedis-mock interrupted while stopping");
            Thread.currentThread().interrupt();
        } finally {
            pool.shutdownNow();
        }
    }

    public boolean isRunning() {
        return service != null;
    }

    public String getHost() {
        RedisServiceJob runningService = Objects.requireNonNull(service, NOT_RUNNING);
        return runningService.getServer().getInetAddress().getHostAddress();
    }

    public int getBindPort() {
        RedisServiceJob runningService = Objects.requireNonNull(service, NOT_RUNNING);
        return runningService.getServer().getLocalPort();
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
                RedisClient rc = new RedisClient(RedisServer.this, socket, clients::remove);
                clients.add(rc);
                threadPool.submit(rc);
            }
            return null;
        }

        public ServerSocket getServer() {
            return server;
        }

        public void stop() throws IOException {
            clients.forEach(RedisClient::close);
            server.close();
            threadPool.shutdownNow();
        }
    }

}
