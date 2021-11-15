package com.github.fppt.jedismock;

import com.github.fppt.jedismock.operations.CommandFactory;
import com.github.fppt.jedismock.operations.RedisCommand;
import com.github.fppt.jedismock.operations.RedisOperation;
import org.junit.jupiter.api.Test;
import org.reflections.Reflections;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.reflections.util.ReflectionUtilsPredicates.withAnnotation;

@Testcontainers
public class SupportedOperationsGeneratorTest {
    private static final String SEPARATOR = " ";
    private static final String HEADING = "# Supported operations:";
    private static final String SYMBOL_SUPPORTED = ":heavy_check_mark:";
    private static final String SYMBOL_UNSUPPORTED = ":x:";
    private static final String REGEX = "\"([^\"]+)\"";

    @Container
    private final GenericContainer redis = new GenericContainer<>(DockerImageName.parse("redis:5.0-alpine"));

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
    public void generate() throws IOException, InterruptedException {
        redis.start();
        final GenericContainer.ExecResult result = redis.execInContainer("redis-cli", "--no-raw", "command");

        Pattern pattern = Pattern.compile(REGEX);
        Matcher matcher = pattern.matcher(result.getStdout());
        List<String> allOperations = new ArrayList<>();
        while (matcher.find()) {
            allOperations.add(matcher.group(1));
        }

        List<String> lines = allOperations.stream()
                .sorted()
                .map(op -> implementedOperations.contains(op) ?
                        SYMBOL_SUPPORTED + SEPARATOR + op:
                        SYMBOL_UNSUPPORTED + SEPARATOR + op
                )
                .collect(Collectors.toList());
        lines.add(0, HEADING);

        writeToFile(lines);
    }
}
