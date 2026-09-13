package com.github.fppt.jedismock;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Class to be used to create a "real" Redis container both in comparison tests and
 * SupportedOperationsGeneratorTest.
 */
public class RedisContainer extends GenericContainer<RedisContainer> {
    private final static DockerImageName IMAGE_NAME = DockerImageName.parse("redis:8.0-alpine");

    public RedisContainer() {
        super(IMAGE_NAME);
    }
}
