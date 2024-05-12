package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.datastructures.Slice;
import com.github.fppt.jedismock.storage.OperationExecutorState;
import com.github.fppt.jedismock.storage.RedisBase;
import org.reflections.Reflections;

import java.lang.reflect.Constructor;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toMap;
import static org.reflections.ReflectionUtils.withAnnotation;

public class CommandFactory {
    private static final Map<Boolean, Map<String, Class<? extends RedisOperation>>> commands;

    static {
        Reflections scanner = new Reflections(CommandFactory.class.getPackage().getName());
        Set<Class<? extends RedisOperation>> redisOperations = scanner.getSubTypesOf(RedisOperation.class);
        commands =
                redisOperations.stream()
                        .filter(withAnnotation(RedisCommand.class))
                        .collect(groupingBy(c -> c.getAnnotation(RedisCommand.class).transactional(),
                                toMap(c -> c.getAnnotation(RedisCommand.class).value(), identity())));
    }

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

    public static void initialize() {
        //This method does nothing, only required for eager static initialization
    }
}
