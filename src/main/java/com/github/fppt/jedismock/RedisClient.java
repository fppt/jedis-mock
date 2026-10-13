package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.exception.EOFException;
import com.github.fppt.jedismock.server.RedisOperationExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.exception.ParseErrorException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.time.Clock;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public final class RedisClient implements Runnable {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisClient.class);
    /**
     * The server whose connection the current thread is serving, while it is
     * running {@link #run()}; lets that server reject lifecycle calls made
     * from inside a command handler.
     */
    private static final ThreadLocal<RedisServer> SERVING = new ThreadLocal<>();
    private final RedisServer server;
    private final Socket socket;
    private final AtomicBoolean running;
    private final RedisOperationExecutor executor;
    private final InputStream in;
    private final OutputStream out;
    private final Consumer<RedisClient> onClose;
    private final long clientId;
    private final Object generation;

    RedisClient(RedisServer server,
                Socket socket,
                Consumer<RedisClient> onClose) throws IOException {
        this(server, socket, onClose, null);
    }

    /**
     * @param generation the generation of the server (its listening service)
     *                   this connection was accepted by; a shutdown it
     *                   requests applies to that generation only.
     */
    RedisClient(RedisServer server,
                Socket socket,
                Consumer<RedisClient> onClose,
                Object generation) throws IOException {
        Objects.requireNonNull(server);
        Objects.requireNonNull(socket);
        Objects.requireNonNull(onClose);
        this.server = server;
        this.clientId = server.nextClientId();
        OperationExecutorState state = new OperationExecutorState(this,
                server.getRedisBases(), server.getBlockingManager(), server.getScriptingManager(),
                server.getConfiguration(), server.getSubscriptionRegistry());
        this.executor = new RedisOperationExecutor(state);
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.running = new AtomicBoolean(true);
        this.onClose = onClose;
        this.generation = generation;
    }

    public void run() {
        SERVING.set(server);
        try {
            while (running.get() && !socket.isClosed() && !Thread.interrupted()) {
                Optional<RedisCommand> command = nextCommand();
                if (command.isPresent()) {
                    Slice response = executor.execCommand(command.get());
                    sendResponse(response, command.toString());
                }
            }
        } finally {
            SERVING.remove();
        }
        LOG.debug("Mock redis connection shut down.");
    }

    /**
     * @return whether the current thread is serving a connection of the given
     * server, i.e. is inside a command handler of it.
     */
    static boolean isServing(RedisServer server) {
        return SERVING.get() == server;
    }

    /**
     * Gets the next command on the stream if one has been issued
     *
     * @return The next command on the stream if one was issues
     */
    private Optional<RedisCommand> nextCommand() {
        try {
            return Optional.of(RedisCommandParser.parse(in));
        } catch (ParseErrorException e) {
            return Optional.empty(); // This simply means there is no next command
        } catch (EOFException e) {
            close();
            return Optional.empty();
        }
    }

    /**
     * Send a response due to a specific command.
     *
     * @param response     The response to send.
     * @param respondingTo The reason for sending this response
     */
    public void sendResponse(Slice response, String respondingTo) {
        if (!running.get()) {
            //The connection was closed from the server side (SHUTDOWN, or
            //stop()): nobody is listening, as with a killed script's error.
            return;
        }
        try {
            if (!response.equals(Response.SKIP)) {
                out.write(response.data());
            }
        } catch (IOException e) {
            LOG.error("unable to send [{}] as response to [{}]", response, respondingTo, e);
        }
    }

    /**
     * Best-effort liveness probe, safe to call from a blocking operation that
     * holds the shared data lock (it neither reads the input stream nor blocks).
     * It sends a single TCP urgent (out-of-band) byte: a live peer silently
     * discards it (SO_OOBINLINE is off by default, so it never enters the RESP
     * stream), while a peer that has gone away makes the write fail. This lets
     * blocking commands (BLPOP, BRPOPLPUSH, blocking XREAD, …) notice that their
     * client disconnected and bail out instead of consuming data meant for a
     * later client on a reused key.
     *
     * @return {@code true} if the connection still appears to be alive.
     */
    public boolean isConnected() {
        if (!running.get() || socket.isClosed() || !socket.isConnected()) {
            return false;
        }
        try {
            socket.sendUrgentData(0xFF);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    /**
     * Close all the streams used by this client effectively closing the client.
     * Also signals the client to stop working.
     */
    public void close() {
        running.set(false);
        executor.cleanup();
        Utils.closeQuietly(socket);
        Utils.closeQuietly(in);
        Utils.closeQuietly(out);
        onClose.accept(this);
    }

    public ServiceOptions options() {
        return server.options();
    }

    /**
     * @return whether this connection has been closed (by the client going
     * away, or by the server stopping), so that a command of its still waiting
     * for the data lock should be dropped rather than executed.
     */
    public boolean isClosed() {
        return !running.get();
    }

    /**
     * Stop the server this connection belongs to, as the {@code SHUTDOWN}
     * command does: every connection, this one included, is closed without a
     * reply and the port is released, and a running script is terminated
     * (whether or not it has written) before the server counts as stopped.
     * Done on a separate thread because stopping the server joins its client
     * threads, and the caller is one of them. A request that runs late, once
     * the generation of the server it was sent to has already been stopped by
     * another one, is ignored.
     */
    public void shutdownServer() {
        Thread shutdown = new Thread(() -> {
            try {
                server.stopRequestedBy(generation);
            } catch (IOException e) {
                LOG.error("Error while shutting the server down", e);
            }
        }, "jedis-mock-shutdown");
        shutdown.setDaemon(true);
        shutdown.start();
    }

    /**
     * @return the server-wide id of this connection, as reported by HELLO.
     */
    public long getClientId() {
        return clientId;
    }

    public int getPort() {
        return socket.getLocalPort();
    }

    public Clock getClock() {
        return server.getClock();
    }

    public String getServerHost() {
        return server.getHost();
    }
}
