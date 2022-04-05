package com.github.fppt.jedismock.server;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.commands.RedisCommand;
import com.github.fppt.jedismock.commands.RedisCommandParser;
import com.github.fppt.jedismock.Utils;
import com.github.fppt.jedismock.exception.ParseErrorException;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

/**
 * Created by Xiaolu on 2015/4/18.
 */
public class RedisClient implements Runnable {
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger(RedisClient.class);
    private final AtomicBoolean running;
    private final RedisOperationExecutor executor;
    private final Socket socket;
    private final ServiceOptions options;
    private final InputStream in;
    private final OutputStream out;
    private final Consumer<RedisClient> onClose;

    RedisClient(Map<Integer, RedisBase> redisBases,
                Socket socket,
                ServiceOptions options,
                Consumer<RedisClient> onClose) throws IOException {
        Objects.requireNonNull(redisBases);
        Objects.requireNonNull(socket);
        Objects.requireNonNull(options);
        Objects.requireNonNull(onClose);
        OperationExecutorState state = new OperationExecutorState(this, redisBases);
        this.executor = new RedisOperationExecutor(state);
        this.socket = socket;
        this.options = options;
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
        }
    }

    /**
     * Send a response due to a specific command.
     *
     * @param response     The respond to send.
     * @param respondingTo The reason for sending this response
     */
    public void sendResponse(Slice response, String respondingTo) {
        try {
            if (!response.equals(Response.SKIP)) {
                out.write(response.data());
            }
        } catch (IOException e) {
            LOG.error("unable to send [" + response + "] as response to [" + respondingTo + "]", e);
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

    ServiceOptions options() {
        return options;
    }
}
