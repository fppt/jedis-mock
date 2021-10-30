package com.github.fppt.jedismock.server;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import java.util.Collections;
import java.util.concurrent.locks.ReentrantLock;

class RedisClientTest {
    Socket s;
    RedisClient redisClient;

    @BeforeEach
    void init() throws IOException {
        s = Mockito.mock(Socket.class);
        Mockito.when(s.getInputStream()).thenReturn(Mockito.mock(InputStream.class));
        Mockito.when(s.getOutputStream()).thenReturn(Mockito.mock(OutputStream.class));
        redisClient = new RedisClient(Collections.emptyMap(), s, ServiceOptions.defaultOptions());
    }

    @Test
    void testClosedSocket() throws IOException {
        Mockito.when(s.isClosed()).thenReturn(true);
        redisClient.run();
    }

}