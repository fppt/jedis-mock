package com.github.fppt.jedismock;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

/**
 * Created by Xiaolu on 2015/4/20.
 */
public class TestUtils {

    @Test
    public void testCloseQuietly() {
        Utils.closeQuietly(null);
        Utils.closeQuietly(new InputStream() {
            @Override
            public int read() {
                return 0;
            }

            @Override
            public void close() throws IOException {
                throw new IOException();
            }
        });
    }

    @Test
    void testReservoirSamplingBasicFunctionality() {
        List<Integer> input = Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8, 9, 10);
        int count = 5;

        List<Integer> sample = Utils.reservoirSampling(input, count, new Random());
        Set<Integer> sampleSet = new HashSet<>(sample);

        assertThat(sample).hasSize(count);
        assertThat(sampleSet).hasSize(count);
        assertThat(input).containsAll(sample);
    }

    @Test
    void testReservoirSamplingWithCountGreaterThanCollection() {
        List<Integer> input = Arrays.asList(1, 2, 3);
        int count = 10;

        List<Integer> sample = Utils.reservoirSampling(input, count, new Random());
        Set<Integer> sampleSet = new HashSet<>(sample);

        assertThat(sample).hasSize(input.size());
        assertThat(sampleSet).hasSize(input.size());
        assertThat(input).containsAll(sample);
    }

    @Test
    void testReservoirSamplingWithExactCount() {
        List<Integer> input = Arrays.asList(1, 2, 3);
        int count = 3;

        List<Integer> sample = Utils.reservoirSampling(input, count, new Random());
        Set<Integer> sampleSet = new HashSet<>(sample);

        assertThat(sample).hasSize(input.size());
        assertThat(sampleSet).hasSize(input.size());
        assertThat(input).containsAll(sample);
    }

    @Test
    void testReservoirSamplingWithZeroCount() {
        List<Integer> input = Arrays.asList(1, 2, 3);
        int count = 0;

        List<Integer> sample = Utils.reservoirSampling(input, count, new Random());

        assertThat(sample).isEmpty();
    }

    @Test
    void testReservoirSamplingWithNegativeCount() {
        List<Integer> input = Arrays.asList(1, 2, 3);
        int count = -1;

        Throwable throwable = catchThrowable(() -> Utils.reservoirSampling(input, count, new Random()));

        assertThat(throwable).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void testReservoirSamplingDeterminismWithFixedSeed() {
        List<Integer> input = new ArrayList<>();
        for (int i = 0; i < 100; i++) input.add(i);
        int count = 10;
        Random random1 = new Random(123);
        Random random2 = new Random(123);

        List<Integer> sample1 = Utils.reservoirSampling(input, count, random1);
        List<Integer> sample2 = Utils.reservoirSampling(input, count, random2);

        assertThat(sample1).isEqualTo(sample2);
    }

}
