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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private static final String OPERATION_NAME_REGEX = "\\r\\n\\s*([^a-z\\s]+)";
    private static final String ANSI_ESCAPE_CODE_REGEX = "\\x1b\\[[0-9;]*[a-zA-Z]";

    @Container
    private final GenericContainer redis = new GenericContainer<>(DockerImageName.parse("redis:6.2-alpine"));

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

        Pattern pattern = Pattern.compile(OPERATION_NAME_REGEX);
        List<String> lines = new ArrayList<>();
        lines.add(HEADING_LEVEL1 + "Supported operations:");
        for (OperationCategory category : OperationCategory.values()) {
            String command = String.format("echo help @%s | redis-cli | sed 's/%s//g'",
                    category.getAnnotationName(), ANSI_ESCAPE_CODE_REGEX);
            final GenericContainer.ExecResult result = redis.execInContainer(
                    "sh", "-c", command);

            Matcher matcher = pattern.matcher(result.getStdout());
            Set<String> categoryOperations = new HashSet<>();
            while (matcher.find()) {
                categoryOperations.add(matcher.group(1).toLowerCase());
            }
            if (categoryOperations.isEmpty()) {
                continue;
            }

            lines.add("");
            lines.add(HEADING_LEVEL2 + category);
            lines.add("");
            lines.addAll(categoryOperations.stream()
                    .sorted()
                    .map(op -> implementedOperations.contains(op) ?
                            SYMBOL_SUPPORTED + SEPARATOR + op + LINE_SEPARATOR :
                            SYMBOL_UNSUPPORTED + SEPARATOR + op + LINE_SEPARATOR
                    )
                    .collect(Collectors.toList()));
        }

        writeToFile(lines);
    }
}
