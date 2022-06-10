package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.storage.RedisBase;

import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by Xiaolu on 2015/4/21.
 */
public class RedisService implements Callable<Void> {

    private final ServerSocket server;
    private final Map<Integer, RedisBase> redisBases;
    private final ServiceOptions options;
    private final ExecutorService threadPool = Executors.newCachedThreadPool();
    private final List<RedisClient> clients = new CopyOnWriteArrayList<>();

    public RedisService(int bindPort, InetAddress address,
                        Map<Integer, RedisBase> redisBases, ServiceOptions options) throws IOException {
        Objects.requireNonNull(redisBases);
        Objects.requireNonNull(options);

        this.server = new ServerSocket(bindPort, 0, address);
        this.redisBases = redisBases;
        this.options = options;
    }

    public Void call() throws IOException {
        while (!server.isClosed()) {
            Socket socket = server.accept();
            RedisClient rc = new RedisClient(redisBases, socket, options, clients::remove);
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
