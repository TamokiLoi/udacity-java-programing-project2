package com.udacity.webcrawler.profiler;

import javax.inject.Inject;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.Writer;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static java.time.format.DateTimeFormatter.RFC_1123_DATE_TIME;

/**
 * Concrete implementation of the {@link Profiler}.
 */
final class ProfilerImpl implements Profiler {
    private final Clock clock;
    private final ZonedDateTime startTime;
    private final ProfilingState state = new ProfilingState();

    @Inject
    ProfilerImpl(Clock clock) {
        this.clock = Objects.requireNonNull(clock, "Clock cannot be null");
        this.startTime = ZonedDateTime.now(clock);
    }


    private Boolean profiledClass(Class<?> klass) {
        List<Method> methods = Arrays.asList(klass.getDeclaredMethods());
        return methods.stream().anyMatch(method -> method.isAnnotationPresent(Profiled.class));
    }

    @Override
    public <T> T wrap(Class<T> klass, T delegate) {
        Objects.requireNonNull(klass);
        if (!profiledClass(klass)) {
            throw new IllegalArgumentException(klass.getName() + " does not have any methods to be profiled.");
        }
        // TODO: Use a dynamic proxy (java.lang.reflect.Proxy) to "wrap" the delegate in a
        //       ProfilingMethodInterceptor and return a dynamic proxy from this method.
        //       See https://docs.oracle.com/javase/10/docs/api/java/lang/reflect/Proxy.html.
        ProfilingMethodInterceptor interceptor = new ProfilingMethodInterceptor(clock, startTime, state, delegate);
        Object proxyInstance = Proxy.newProxyInstance(
                ProfilerImpl.class.getClassLoader(),
                new Class<?>[]{klass},
                interceptor
        );

        return klass.cast(proxyInstance);
    }

    @Override
    public void writeData(Path path) {
        // TODO: Write the ProfilingState data to the given file path. If a file already exists at that
        //       path, the new data should be appended to the existing file.
        Objects.requireNonNull(path);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            writeData(writer);
        } catch (IOException e) {
            System.err.println("Error writing data to file: " + e.getMessage());
            e.printStackTrace();
        }
    }

    @Override
    public void writeData(Writer writer) throws IOException {
        writer.write("Run at " + RFC_1123_DATE_TIME.format(startTime));
        writer.write(System.lineSeparator());
        state.write(writer);
        writer.write(System.lineSeparator());
    }
}