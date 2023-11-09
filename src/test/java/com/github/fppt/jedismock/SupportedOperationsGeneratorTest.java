package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import com.github.fppt.jedismock.util.OperationCategory;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.reflections.util.ReflectionUtilsPredicates.withAnnotation;

@Testcontainers
public class SupportedOperationsGeneratorTest {
    private static final String SEPARATOR = " ";
    private static final String LINE_SEPARATOR = "<br>";
    private static final String HEADING_LEVEL1 = "# ";
    private static final String HEADING_LEVEL2 = "## ";
    private static final String SYMBOL_SUPPORTED = ":heavy_check_mark:";
    private static final String SYMBOL_UNSUPPORTED = ":x:";
    @Container
    private final GenericContainer<?> redis =
            new GenericContainer<>(DockerImageName.parse("redis:7.2-alpine")).withExposedPorts(6379);

    private final static Set<String> implementedOperations;

    static {
        Reflections scanner = new Reflections(CommandFactory.class.getPackage().getName());
        Set<Class<? extends RedisOperation>> redisOperations = scanner.getSubTypesOf(RedisOperation.class);
        implementedOperations =
                redisOperations.stream()
                        .filter(withAnnotation(RedisCommand.class))
                        .map(op -> op.getAnnotation(RedisCommand.class).value())
                        .collect(Collectors.toSet());
    }

    private void writeToFile(List<String> lines) throws IOException {
        Path path = Paths.get(System.getProperty("user.dir"), "supported_operations.md");
        if (!Files.exists(path)) {
            Files.createFile(path);
        }
        Files.write(path, lines, StandardCharsets.UTF_8);
    }

    @Test
    public void generate() throws IOException {
        redis.start();

        List<String> lines = new ArrayList<>();
        lines.add(HEADING_LEVEL1 + "Supported operations:");

        Set<String> mentioned = new HashSet<>();

        try (Jedis jedis = new Jedis(redis.getHost(), redis.getFirstMappedPort())) {
            for (OperationCategory category : OperationCategory.values()) {
                final List<String> categoryOperations = jedis.aclCat(category.getAnnotationName());


                lines.add("");
                lines.add(HEADING_LEVEL2 + category);
                lines.add("");
                lines.addAll(categoryOperations.stream()
                        .map(s ->
                                {
                                    int indexOfPipe = s.indexOf('|');
                                    return indexOfPipe > 0 ? s.substring(0, indexOfPipe) : s;
                                }
                        )
                        .filter(s -> !mentioned.contains(s))
                        .peek(mentioned::add)
                        .sorted()
                        .map(op -> implementedOperations.contains(op) ?
                                SYMBOL_SUPPORTED + SEPARATOR + op + LINE_SEPARATOR :
                                SYMBOL_UNSUPPORTED + SEPARATOR + op + LINE_SEPARATOR
                        )
                        .collect(Collectors.toList()));
            }
        }
        writeToFile(lines);


    }
}
