package com.github.fppt.jedismock.comparisontests.cluster;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

@ExtendWith(ComparisonBase.class)
public class TestCluster {
    @TestTemplate
    void testClusterInNonClusterMode(Jedis jedis) {
        String msg = Assertions.assertThrows(JedisDataException.class,
                jedis::clusterMyId).getMessage();
        Assertions.assertEquals("ERR This instance has cluster support disabled", msg);
    }
}
