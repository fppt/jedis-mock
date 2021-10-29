package com.github.fppt.jedismock.operations;

import com.github.fppt.jedismock.server.Slice;
import org.reflections.Reflections;

import java.lang.annotation.Annotation;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.*;

public class ReflectionOperationFactory<T> implements RedisOperationFactory<T> {
    private final Reflections scanner = new Reflections(ReflectionOperationFactory.class.getPackage().getName());
    private final Map<String, Class<? extends RedisOperation>> operations = new HashMap<>();

    public ReflectionOperationFactory(Class<? extends Annotation> annotationType) {
        Set<Class<?>> annotatedClasses = scanner.getTypesAnnotatedWith(annotationType, true);

        Set<Class<? extends RedisOperation>> classes = scanner.getSubTypesOf(RedisOperation.class);
        classes.retainAll(annotatedClasses);

        for (Class<? extends RedisOperation> aClass : classes) {
            Annotation annotation = aClass.getAnnotation(annotationType);
            Method method = annotation.annotationType().getMethods()[0];
            try {
                String operationName = (String) method.invoke(annotation);
                operations.put(operationName, aClass);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
    }

    @Override
    public Optional<RedisOperation> buildOperation(String name, T base, List<Slice> params) {
        if (operations.containsKey(name)) {
            Class<? extends RedisOperation> aClass = operations.get(name);

            Constructor<?> constructor = aClass.getDeclaredConstructors()[0];
            Object[] constructorParams = Arrays.asList(base, params).subList(0, constructor.getParameterCount()).toArray();

            try {
                RedisOperation operation = (RedisOperation) constructor.newInstance(constructorParams);
                return Optional.of(operation);
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
        }
        return Optional.empty();
    }
}
