package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.server.RedisService;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
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
    private volatile ExecutorService threadPool;
    private volatile RedisService service;
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

    public RedisServer start() throws IOException {
        if (!(service == null)) {
            throw new IllegalStateException();
        }
        this.service = new RedisService(bindPort, bindAddress, redisBases, options);
        threadPool = Executors.newSingleThreadExecutor();
        serviceFinalization = threadPool.submit(service);
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
            threadPool.shutdownNow();
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
}
