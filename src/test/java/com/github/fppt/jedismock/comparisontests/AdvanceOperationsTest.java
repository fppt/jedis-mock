package com.github.fppt.jedismock.comparisontests;


import com.github.fppt.jedismock.util.MockSubscriber;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Client;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPubSub;
import redis.clients.jedis.Transaction;
import redis.clients.jedis.exceptions.JedisDataException;

import java.util.List;
import java.util.concurrent.*;

import static org.junit.jupiter.api.Assertions.*;

@ExtendWith(ComparisonBase.class)
public class AdvanceOperationsTest {

    @TestTemplate
    public void whenTransactionWithMultiplePushesIsExecuted_EnsureResultsAreSaved(Jedis jedis) {
        String key = "my-list";
        assertEquals(new Long(0), jedis.llen(key));

        Transaction transaction = jedis.multi();
        transaction.lpush(key, "1");
        transaction.lpush(key, "2");
        transaction.lpush(key, "3");
        transaction.exec();

        assertEquals(new Long(3), jedis.llen(key));
    }

    @TestTemplate
    public void whenUsingTransactionAndTryingToAccessJedis_Throw(Jedis jedis) {
        //Do Something random with Jedis
        assertNull(jedis.get("oobity-oobity-boo"));

        //Start transaction
        jedis.multi();
        assertEquals("Cannot use Jedis when in Multi. Please use Transaction or reset jedis state.",
                assertThrows(JedisDataException.class, () ->
                        jedis.get("oobity-oobity-boo")).getMessage());
    }

    @TestTemplate
    public void whenSubscribingToAChannel_EnsurePublishedMessagesAreReceived(Jedis jedis) throws InterruptedException, ExecutionException {
        String channel = "normalbob";
        String message = "SUPERBOB";

        //Create subscriber
        ExecutorService subsciberThread = Executors.newSingleThreadExecutor();
        MockSubscriber mockSubscriber = new MockSubscriber();

        Client client = jedis.getClient();
        Jedis subscriber = new Jedis(client.getHost(), client.getPort());
        CountDownLatch subscribeLatch = new CountDownLatch(1);
        Future<?> submit = subsciberThread.submit(() -> {
            subscribeLatch.countDown();
            subscriber.subscribe(mockSubscriber, channel);

        });
        subscribeLatch.await();
        //Give some time to subscribe
        Thread.sleep(100);

        //publish message
        jedis.publish(channel, message);
        //Getters block until message goes through
        assertEquals(channel, mockSubscriber.latestChannel());
        assertEquals(message, mockSubscriber.latestMessage());
        mockSubscriber.unsubscribe();
        submit.get();
    }

    @TestTemplate
    public void whenUsingBrpoplpush_EnsureItBlocksAndCorrectResultsAreReturned(Jedis jedis) throws ExecutionException, InterruptedException {
        String list1key = "source list";
        String list2key = "target list";

        jedis.rpush(list2key, "a", "b", "c");

        //Block on performing the BRPOPLPUSH
        Client client = jedis.getClient();
        Jedis blockedClient = new Jedis(client.getHost(), client.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            String result = blockedClient.brpoplpush(list1key, list2key, 500);
            assertEquals("3", result);
        });

        //Check the list is not modified
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertEquals(3, results.size());

        //Push some stuff into the blocked list
        jedis.rpush(list1key, "1", "2", "3");

        future.get();

        //Check the list is modified
        results = jedis.lrange(list2key, 0, -1);
        assertEquals(4, results.size());
    }

    @TestTemplate
    public void whenUsingBrpoplpushAndReachingTimeout_Return(Jedis jedis) {
        String list1key = "another source list";
        String list2key = "another target list";

        String result = jedis.brpoplpush(list1key, list2key, 1);

        assertNull(result);
    }

    @TestTemplate
    public void whenUsingBrpoplpush_EnsureClientCanStillGetOtherResponsesInTimelyManner(Jedis jedis) {
        String list1key = "another another source list";
        String list2key = "another another target list";

        Client client = jedis.getClient();
        Jedis blockedClient = new Jedis(client.getHost(), client.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        blockingThread.submit(() -> {
            String result = blockedClient.brpoplpush(list1key, list2key, 500);
            assertEquals("3", result);
        });

        //Issue random commands to make sure mock is still responsive
        jedis.set("k1", "v1");
        jedis.set("k2", "v2");
        jedis.set("k3", "v3");
        jedis.set("k4", "v4");
        jedis.set("k5", "v5");

        //Check random commands were processed
        assertEquals("v1", jedis.get("k1"));
        assertEquals("v2", jedis.get("k2"));
        assertEquals("v3", jedis.get("k3"));
        assertEquals("v4", jedis.get("k4"));
        assertEquals("v5", jedis.get("k5"));
    }

    @Disabled("This test fails with the embedded redis sometimes so I am at a loss")
    @TestTemplate
    public void whenSubscribingToAChannelAndThenUnsubscribing_EnsureAllChannelsAreUbSubScribed(Jedis jedis) throws InterruptedException {
        String channel1 = "normaltim1";
        String channel2 = "normaltim2";
        //String channel3 = "normaltim3";
        String message = "SUPERTIM";

        //Create Subscriber
        ExecutorService subsciberThread = Executors.newCachedThreadPool();
        Client client = jedis.getClient();
        Jedis subscriber = new Jedis(client.getHost(), client.getPort());

        subscriber.set("thing", "a-thing");
        assertEquals("a-thing", subscriber.get("thing"));

        //Subscribe to Channels
        subsciberThread.submit(() -> subscribeToChannel(subscriber, channel1));
        subsciberThread.submit(() -> subscribeToChannel(subscriber, channel2));
        //subsciberThread.submit(() -> subscribeToChannel(subscriber, channel3));

        //Give some time to subscribe
        Thread.sleep(5000);

        //publish messages which trigger the unsubcription
        jedis.publish(channel1, message);
        jedis.publish(channel2, message);
        //jedis.publish(channel3, message);

        //Give some time for publications to go through
        Thread.sleep(2000);

        subscriber.set("thing", "a-new-thing");
    }

    private static void subscribeToChannel(Jedis subscriber, String channel) {
        System.out.println("Subscribing to :" + channel);
        subscriber.subscribe(new JedisPubSub() {
            @Override
            public void onMessage(String channel, String message) {
                System.out.println("Unsubscribing from :" + channel);
                unsubscribe(channel);
            }
        }, channel);
    }

    @TestTemplate
    public void whenChangingBetweenRedisDBS_EnsureChangesAreMutuallyExclusive(Jedis jedis) {
        String key1 = "k1";
        String key2 = "k2";

        String val1 = "v1";
        String val2 = "v2";
        String val3 = "v3";

        //Mess With Default Cluster
        jedis.set(key1, val1);
        jedis.set(key2, val2);
        assertEquals(val1, jedis.get(key1));
        assertEquals(val2, jedis.get(key2));

        //Change to new DB
        jedis.select(2);
        assertNull(jedis.get(key1));
        assertNull(jedis.get(key2));

        jedis.set(key1, val3);
        jedis.set(key2, val3);
        assertEquals(val3, jedis.get(key1));
        assertEquals(val3, jedis.get(key2));

        //Change back and make sure original is unchanged
        jedis.select(0);
        assertEquals(val1, jedis.get(key1));
        assertEquals(val2, jedis.get(key2));
    }


    @TestTemplate
    public void whenUsingBlpop_EnsureItBlocksAndCorrectResultsAreReturned(Jedis jedis) throws ExecutionException, InterruptedException {
        String key = "list1_kfubdjkfnv";
        jedis.rpush(key, "d", "e", "f");
        //Block on performing the BLPOP
        Client client = jedis.getClient();
        Jedis blockedClient = new Jedis(client.getHost(), client.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(10, key);
            assertEquals(2, result.size());
            assertEquals(key, result.get(0));
            assertEquals("d", result.get(1));
        });
        future.get();
        //Check the list is modified
        List<String> results = jedis.lrange(key, 0, -1);
        assertEquals(2, results.size());
    }


    @TestTemplate
    public void whenUsingBlpop_EnsureItBlocksAndCorrectResultsAreReturnedOnMultipleList(Jedis jedis) throws ExecutionException, InterruptedException {
        String list1key = "list1_dkjfnvdk";
        String list2key = "list2_kjvnddkf";
        String list3key = "list3_oerurthv";


        //Block on performing the BLPOP
        Client client = jedis.getClient();
        Jedis blockedClient = new Jedis(client.getHost(), client.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(10, list1key, list2key, list3key);
            assertEquals(list2key, result.get(0));
            assertEquals("a", result.get(1));
        });
        Thread.sleep(1000);
        jedis.rpush(list2key, "a", "b", "c");
        jedis.rpush(list3key, "d", "e", "f");
        future.get();

        //Check the list is modified
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertEquals(2, results.size());
        results = jedis.lrange(list3key, 0, -1);
        assertEquals(3, results.size());
    }

    @TestTemplate
    public void whenUsingBlpop_EnsureItTimeout(Jedis jedis) throws ExecutionException, InterruptedException, TimeoutException {
        String list1key = "list1_kdjfnvdsu";
        String list2key = "list2_mbhkdushy";
        String list3key = "list3_qzkmpthju";

        // init redisbase
        jedis.lrange(list2key, 0, -1);

        //Block on performing the BLPOP
        Client client = jedis.getClient();
        Jedis blockedClient = new Jedis(client.getHost(), client.getPort());
        ExecutorService blockingThread = Executors.newSingleThreadExecutor();
        Future future = blockingThread.submit(() -> {
            List<String> result = blockedClient.blpop(1, list1key, list2key, list3key);
            assertNull(result);
        });
        //Check the list is not modified
        client.setSoTimeout(2000);
        List<String> results = jedis.lrange(list2key, 0, -1);
        assertEquals(0, results.size());
        future.get(4, TimeUnit.SECONDS);
    }
}
