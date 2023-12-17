package com.github.fppt.jedismock.comparisontests.cluster;

import com.github.fppt.jedismock.comparisontests.ComparisonBase;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.exceptions.JedisDataException;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(ComparisonBase.class)
public class TestCluster {
    @TestTemplate
    void testClusterInNonClusterMode(Jedis jedis) {
        assertThatThrownBy(jedis::clusterMyId)
                .isInstanceOf(JedisDataException.class)
                .hasMessage("ERR This instance has cluster support disabled");
    }
}
