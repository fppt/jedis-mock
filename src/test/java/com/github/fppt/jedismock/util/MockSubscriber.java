package com.github.fppt.jedismock.util;

import redis.clients.jedis.JedisPubSub;

import java.util.concurrent.CountDownLatch;

public class MockSubscriber extends JedisPubSub {

    private String latestReceivedFromChannel;
    private String latestReceivedMessage;
    private CountDownLatch latch = new CountDownLatch(1);

    @Override
    public void onMessage(String channel, String message) {
        latestReceivedFromChannel = channel;
        latestReceivedMessage = message;
        latch.countDown();
    }

    public String latestChannel() throws InterruptedException {
        latch.await();
        return latestReceivedFromChannel;
    }

    public synchronized String latestMessage() throws InterruptedException {
        latch.await();
        return latestReceivedMessage;
    }

}
