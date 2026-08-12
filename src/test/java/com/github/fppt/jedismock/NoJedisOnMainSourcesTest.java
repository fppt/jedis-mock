package com.github.fppt.jedismock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * jedis is a test-only dependency: it must never appear on the compile
 * classpath, or it lands transitively on every consumer's classpath.
 */
class NoJedisOnMainSourcesTest {

    private static final Path MAIN_SOURCES =
            Paths.get(System.getProperty("user.dir"), "src", "main", "java");

    @Test
    void noMainSourceReferencesJedis() throws IOException {
        try (Stream<Path> paths = Files.walk(MAIN_SOURCES)) {
            List<String> offenders = paths
                    .filter(p -> p.toString().endsWith(".java"))
                    .filter(NoJedisOnMainSourcesTest::mentionsJedis)
                    .map(MAIN_SOURCES::relativize)
                    .map(Path::toString)
                    .sorted()
                    .collect(Collectors.toList());

            assertThat(offenders)
                    .as("Main sources must not reference redis.clients.* — "
                            + "jedis is a test-scoped dependency")
                    .isEmpty();
        }
    }

    private static boolean mentionsJedis(Path source) {
        try {
            return new String(Files.readAllBytes(source), StandardCharsets.UTF_8)
                    .contains("redis.clients");
        } catch (IOException e) {
            throw new IllegalStateException("Cannot read " + source, e);
        }
    }
}
