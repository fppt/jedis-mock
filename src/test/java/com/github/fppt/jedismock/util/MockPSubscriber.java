package com.github.fppt.jedismock.util;

import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class MockPSubscriber extends JedisPubSub {

    private String latestReceivedFromChannel;
    private String latestReceivedFromPattern;
    private String latestReceivedMessage;
    private final CountDownLatch latch = new CountDownLatch(1);
    
    @Override
    public void onPMessage(String pattern, String channel, String message) {
        latestReceivedFromChannel = channel;
        latestReceivedMessage = message;
        latestReceivedFromPattern = pattern;
        latch.countDown();
    }

    public String latestChannel() throws InterruptedException {
        latch.await(10, TimeUnit.SECONDS);
        return latestReceivedFromChannel;
    }

    public String latestPattern() throws InterruptedException {
        latch.await(10, TimeUnit.SECONDS);
        return latestReceivedFromPattern;
    }

    public String latestMessage() throws InterruptedException {
        latch.await(10, TimeUnit.SECONDS);
        return latestReceivedMessage;
    }
}
