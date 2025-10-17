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
    private final RedisServer server;
    private final Socket socket;
    private final AtomicBoolean running;
    private final RedisOperationExecutor executor;
    private final InputStream in;
    private final OutputStream out;
    private final Consumer<RedisClient> onClose;

    RedisClient(RedisServer server,
                Socket socket,
                Consumer<RedisClient> onClose) throws IOException {
        Objects.requireNonNull(server);
        Objects.requireNonNull(socket);
        Objects.requireNonNull(onClose);
        this.server = server;
        OperationExecutorState state = new OperationExecutorState(this,
                server.getRedisBases());
        this.executor = new RedisOperationExecutor(state);
        this.socket = socket;
        this.in = socket.getInputStream();
        this.out = socket.getOutputStream();
        this.running = new AtomicBoolean(true);
        this.onClose = onClose;
    }

    public void run() {
        while (running.get() && !socket.isClosed() && !Thread.interrupted()) {
            Optional<RedisCommand> command = nextCommand();
            if (command.isPresent()) {
                Slice response = executor.execCommand(command.get());
                sendResponse(response, command.toString());
            }
        }
        LOG.debug("Mock redis connection shut down.");
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
        try {
            if (!response.equals(Response.SKIP)) {
                out.write(response.data());
            }
        } catch (IOException e) {
            LOG.error("unable to send [{}] as response to [{}]", response, respondingTo, e);
        }
    }

    /**
     * Close all the streams used by this client effectively closing the client.
     * Also signals the client to stop working.
     */
    public void close() {
        running.set(false);
        Utils.closeQuietly(socket);
        Utils.closeQuietly(in);
        Utils.closeQuietly(out);
        onClose.accept(this);
    }

    public ServiceOptions options() {
        return server.options();
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
