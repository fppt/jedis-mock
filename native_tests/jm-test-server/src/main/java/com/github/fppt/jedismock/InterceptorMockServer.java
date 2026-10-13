package com.github.fppt.jedismock;

import com.github.fppt.jedismock.datastructures.RMDataStructure;
import com.github.fppt.jedismock.datastructures.RMHash;
import com.github.fppt.jedismock.datastructures.RMString;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.operations.server.MockExecutor;
import com.github.fppt.jedismock.server.Response;
import com.github.fppt.jedismock.server.ServiceOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static com.github.fppt.jedismock.Utils.convertToLong;

public final class InterceptorMockServer {

    public static final int PORT = 39807;
    public static final Logger LOGGER = LoggerFactory.getLogger(InterceptorMockServer.class);
    public static final int SHARED_OBJECT_THRESHOLD = 10000;
    /** How often to check whether a SHUTDOWN has stopped the server. */
    private static final long SHUTDOWN_POLL_MILLIS = 100L;
    /** Pause before restarting after a SHUTDOWN, so that tests observe the port being closed. */
    private static final long RESTART_DELAY_MILLIS = 1000L;

    private InterceptorMockServer() {

    }

    public static void main(String[] args) throws IOException, InterruptedException {
        LOGGER.info("Service starting...");
        RedisServer server = RedisServer
                .newRedisServer(PORT)
                .setOptions(ServiceOptions.withInterceptor((state, roName, params) -> {
                    if ("debug".equalsIgnoreCase(roName)
                            && "object".equalsIgnoreCase(params.get(0).toString())
                    ) {
                        // Handling unsopported DEBUG OBJECT command
                        RMDataStructure value = state.base().getValue(params.get(1));

                        // Currently it is supported only for RMHash.
                        // If you will add support for other data structures, insert their handling here
                        if (value instanceof RMHash) {
                            return Response.bulkString(Slice.create("hashtable"));
                        } else if (value instanceof RMString) {
                            return Response.bulkString(Slice.create(" at: "));
                        }
                        return Response.bulkString(
                                Slice.create("DEBUG OBJECT command for this data structure is not yet supported")
                        );
                    } else if ("debug".equalsIgnoreCase(roName)
                            && "reload".equalsIgnoreCase(params.get(0).toString())) {
                        return Response.OK;
                    } else if ("object".equalsIgnoreCase(roName)
                            && "encoding".equalsIgnoreCase(params.get(0).toString())
                    ) {
                        // Handling unsopported OBJECT ENCODING command
                        RMDataStructure value = state.base().getValue(params.get(1));

                        // Currently it is supported only for RMHash.
                        // If you will add support for other data structures, insert their handling here
                        if (value instanceof RMHash) {
                            RMHash hash = (RMHash) value;
                            return Response.bulkString(Slice.create("hashtable"));
                        }
                        return Response.bulkString(
                                Slice.create("OBJECT ENCODING command for this data structure is not yet supported")
                        );
                    } else if ("object".equalsIgnoreCase(roName)
                            && "refcount".equalsIgnoreCase(params.get(0).toString())
                    ) {
                        //Imitate shared objects
                        long val = convertToLong(state.base().getRMString(params.get(1)).getStoredDataAsString());
                        if (val < SHARED_OBJECT_THRESHOLD) return
                                Response.integer(2);
                        else return Response.integer(1);
                    } else {
                        //Delegate execution to JedisMock which will mock the real Redis behaviour (when it can)
                        return MockExecutor.proceed(state, roName, params);
                    }
                }));
        //A SHUTDOWN sent by a test stops the server, which is what that test
        //expects to observe (connection refused). The following test stanzas
        //expect the server back, so restart it after a pause long enough for
        //the observation; the harness reconnects once the port is up again.
        while (true) {
            server.start();
            LOGGER.info("Service started at port {}.", PORT);
            while (server.isRunning()) {
                Thread.sleep(SHUTDOWN_POLL_MILLIS);
            }
            LOGGER.info("Service stopped by SHUTDOWN, restarting in {} ms", RESTART_DELAY_MILLIS);
            Thread.sleep(RESTART_DELAY_MILLIS);
        }
    }
}
