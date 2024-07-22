package com.github.fppt.jedismock;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;

class RedisClientTest {
    Socket s;
    RedisClient redisClient;

    @BeforeEach
    void init() throws IOException {
        s = Mockito.mock(Socket.class);
        Mockito.when(s.getInputStream()).thenReturn(Mockito.mock(InputStream.class));
        Mockito.when(s.getOutputStream()).thenReturn(Mockito.mock(OutputStream.class));
        redisClient = new RedisClient(Mockito.mock(RedisServer.class), s, c -> {
        });
    }

    @Test
    void testClosedSocket() {
        Mockito.when(s.isClosed()).thenReturn(true);
        redisClient.run();
    }

}