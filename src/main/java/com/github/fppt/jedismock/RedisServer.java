package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.server.RedisService;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.function.BiFunction;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class RedisServer {

    private final int bindPort;
    private final Map<Integer, RedisBase> redisBases;
    private final ExecutorService threadPool;
    private RedisService service;
    private ServiceOptions options = ServiceOptions.defaultOptions();
    private Future<Void> serviceFinalization;
    private BiFunction<String, Slice, Slice> mockedOperationsHandler;

    public RedisServer() throws IOException {
        this(0);
        mockedOperationsHandler =
            (cmd, params) -> Response.error(String.format("Unsupported operation: pubsub %s", cmd));
    }

    public RedisServer(int port) {
        this.bindPort = port;
        this.redisBases = new HashMap<>();
        this.threadPool = Executors.newSingleThreadExecutor();
        mockedOperationsHandler =
                (cmd, params) -> Response.error(String.format("Unsupported operation: pubsub %s", cmd));
        CommandFactory.initialize();
    }

    static public RedisServer newRedisServer() throws IOException {
        return new RedisServer();
    }

    static public RedisServer newRedisServer(int port) throws IOException {
        return new RedisServer(port);
    }

    public void setOptions(ServiceOptions options) {
        Objects.requireNonNull(options);
        this.options = options;
    }

    /**
     * Set mockedOperationsHandler which handles operations are not supported or overrides standard behaviour.
     * @param mockedOperationsHandler - functor, which takes RedisOperation and overrides
     *                               behavior of RedisOperationExecutor.
     */
    public void setMockedCommands(BiFunction<String, Slice, Slice> mockedOperationsHandler) {
        this.mockedOperationsHandler = mockedOperationsHandler;
    }

    public void start() throws IOException {
        if (!(service == null)) {
            throw new IllegalStateException();
        }
        this.service = new RedisService(bindPort, redisBases, options);
        this.service.setMockedOperationHandler(mockedOperationsHandler);
        serviceFinalization = threadPool.submit(service);
    }

    public void stop() throws IOException {
        Objects.requireNonNull(service);
        service.stop();
        try {
            serviceFinalization.get();
        } catch (ExecutionException e) {
            //Do nothing: it's a normal behaviour when the service was stopped
        } catch (InterruptedException e){
            System.err.println("Jedis-mock interrupted while stopping");
            Thread.currentThread().interrupt();
        }
    }

    public String getHost() {
        Objects.requireNonNull(service);
        return service.getServer().getInetAddress().getHostAddress();
    }

    public int getBindPort() {
        Objects.requireNonNull(service);
        return service.getServer().getLocalPort();
    }
}
