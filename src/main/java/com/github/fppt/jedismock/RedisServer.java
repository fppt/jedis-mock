package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.RedisBase;

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
    private final InetAddress bindAddress;
    private final Map<Integer, RedisBase> redisBases;
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
        Objects.requireNonNull(options);
        this.options = options;
        return this;
    }

    public RedisServer setClock(Clock clock) {
        Objects.requireNonNull(clock);
        this.clock = clock;
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
        Objects.requireNonNull(service, NOT_RUNNING);
        service.stop();
        service = null;
        try {
            serviceFinalization.get();
        } catch (ExecutionException e) {
            //Do nothing: it's a normal behaviour when the service was stopped
        } catch (InterruptedException e) {
            System.err.println("Jedis-mock interrupted while stopping");
            Thread.currentThread().interrupt();
        } finally {
            singleThreadPool.shutdownNow();
        }
    }

    public boolean isRunning() {
        return service != null;
    }

    public String getHost() {
        Objects.requireNonNull(service, NOT_RUNNING);
        return service.getServer().getInetAddress().getHostAddress();
    }

    public int getBindPort() {
        Objects.requireNonNull(service, NOT_RUNNING);
        return service.getServer().getLocalPort();
    }

    Map<Integer, RedisBase> getRedisBases() {
        return redisBases;
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
