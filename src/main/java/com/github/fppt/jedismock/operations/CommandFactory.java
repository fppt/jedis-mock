package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.RedisClient;
import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import com.github.fppt.jedismock.storage.SubscriptionRegistry;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;

public class CommandFactory {
    private static final Map<Boolean, Map<String, Class<? extends RedisOperation>>> commands;

    static {
        commands =
                StreamSupport.stream(registries().spliterator(), false)
                        .map(CommandRegistry::commands)
                        .flatMap(List::stream)
                        .collect(groupingBy(c -> c.getAnnotation(RedisCommand.class).transactional(),
                                toMap(c -> c.getAnnotation(RedisCommand.class).value(), identity())));
    }

    /**
     * Instantiates the operation class registered for the command, resolving
     * every constructor argument by its type:
     * <ul>
     * <li>{@link List} — the command parameters;</li>
     * <li>{@link OperationExecutorState} — the connection state;</li>
     * <li>{@link RedisBase} — the currently selected database;</li>
     * <li>{@link SubscriptionRegistry} — the server-wide pub/sub registry;</li>
     * <li>{@link RedisClient} — the client issuing the command.</li>
     * </ul>
     * Prefer the narrow types over {@code OperationExecutorState}: the
     * constructor signature then documents exactly what the command touches.
     */
    public static RedisOperation buildOperation(String name, boolean transactional,
                                                          OperationExecutorState state, List<Slice> params) {
        Class<? extends RedisOperation> commandClass = commands.get(transactional).get(name);
        if (commandClass != null) {
            try {
                Constructor<?> declaredConstructor = commandClass.getDeclaredConstructors()[0];
                Class<?>[] parameterTypes = declaredConstructor.getParameterTypes();
                Constructor<? extends RedisOperation> constructor = commandClass.getDeclaredConstructor(parameterTypes);
                constructor.setAccessible(true);
                Object[] parameters = new Object[parameterTypes.length];
                for (int i = 0; i < parameterTypes.length; i++) {
                    if (parameterTypes[i].isAssignableFrom(List.class)) {
                        parameters[i] = params;
                    } else if (parameterTypes[i].isAssignableFrom(OperationExecutorState.class)) {
                        parameters[i] = state;
                    } else if (parameterTypes[i].isAssignableFrom(RedisBase.class)) {
                        parameters[i] = state.base();
                    } else if (parameterTypes[i].isAssignableFrom(SubscriptionRegistry.class)) {
                        parameters[i] = state.subscriptionRegistry();
                    } else if (parameterTypes[i].isAssignableFrom(RedisClient.class)) {
                        parameters[i] = state.owner();
                    } else {
                        throw new IllegalArgumentException(String.format(
                                "Cannot resolve parameter of type %s for command %s",
                                parameterTypes[i].getSimpleName(), name));
                    }
                }
                return constructor.newInstance(parameters);
            } catch (ReflectiveOperationException e) {
                throw new IllegalStateException(e);
            }
        } else {
            return null;
        }
    }

    /**
     * Every operation class registered with this factory.
     *
     * <p>Exposed so that tests can check the registries for drift and so that
     * the supported-operations documentation can be generated without a second,
     * independent scan.
     */
    public static Set<Class<? extends RedisOperation>> registeredCommandClasses() {
        return commands.values().stream()
                .flatMap(byName -> byName.values().stream())
                .collect(Collectors.toSet());
    }

    /**
     * The per-package registries, discovered through {@link ServiceLoader}
     * from META-INF/services rather than by scanning the classpath.
     *
     * <p>Loaded with this class's own class loader instead of the thread
     * context one, so discovery does not depend on which thread first touches
     * the factory.
     */
    private static ServiceLoader<CommandRegistry> registries() {
        return ServiceLoader.load(CommandRegistry.class, CommandFactory.class.getClassLoader());
    }

    public static void initialize() {
        //This method does nothing, only required for eager static initialization
    }
}
