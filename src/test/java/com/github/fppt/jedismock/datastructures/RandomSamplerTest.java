package com.github.fppt.jedismock.datastructures;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RandomSamplerTest {
    private static final Random NO_RANDOM = new Random() {
        @Override
        public int nextInt(int bound) {
            throw new AssertionError("nextInt should not be called");
        }
    };

    @Test
    void samplesEdgeCases() {
        Set<Integer> source = IntStream.range(0, 1000).boxed()
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Integer> before = new LinkedHashSet<>(source);
        for (int count : new int[]{1, 3, 999, 1000, 1001}) {
            List<Integer> result = RandomSampler.sampleDistinct(source, count, getRandom());
            assertThat(result).hasSize(Math.min(count, source.size())).doesNotHaveDuplicates();
            assertThat(source).containsAll(result);
        }
        assertThat(source).containsExactlyElementsOf(before);
    }

    @Test
    void supportsMoreDrawsThanMembersWithReplacement() {
        List<String> source = Arrays.asList("a", "b", "c");
        List<String> result = RandomSampler.sampleWithReplacement(source, 1000, getRandom());
        assertThat(result).hasSize(1000).containsOnly("a", "b", "c");
        assertThat(source).containsExactly("a", "b", "c");
        assertThat(RandomSampler.sampleWithReplacement(Collections.singleton("a"), 3, getRandom()))
                .containsExactly("a", "a", "a");
    }

    @Test
    void resetsTheProvidedGeneratorForReplay() {
        Set<String> members = new LinkedHashSet<>(Arrays.asList("a", "b", "c", "d", "e", "f"));
        Random random = new Random(42);
        List<String> distinct = RandomSampler.sampleDistinct(members, 4, random);
        List<String> repeated = RandomSampler.sampleWithReplacement(members, 50, random);

        random.setSeed(42);
        assertThat(RandomSampler.sampleDistinct(members, 4, random)).containsExactlyElementsOf(distinct);
        assertThat(RandomSampler.sampleWithReplacement(members, 50, random)).containsExactlyElementsOf(repeated);
    }

    @Test
    void returnsEmptyForEmptySourcesAndZeroCountWithoutUsingRandom() {
        Set<String> source = Collections.singleton("a");
        assertThat(RandomSampler.sampleDistinct(source, 0, NO_RANDOM)).isEmpty();
        assertThat(RandomSampler.sampleWithReplacement(source, 0, NO_RANDOM)).isEmpty();
        for (int count : new int[]{0, 3}) {
            assertThat(RandomSampler.sampleDistinct(Collections.emptyList(), count, NO_RANDOM)).isEmpty();
            assertThat(RandomSampler.sampleWithReplacement(Collections.emptyList(), count, NO_RANDOM)).isEmpty();
        }
    }

    @Test
    void rejectsNegativeCountsWithoutUsingRandom() {
        Set<String> source = Collections.singleton("a");
        for (int count : new int[]{-1, Integer.MIN_VALUE}) {
            assertThatThrownBy(() -> RandomSampler.sampleDistinct(source, count, NO_RANDOM))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> RandomSampler.sampleWithReplacement(source, count, NO_RANDOM))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> RandomSampler.sampleDistinct(Collections.emptyList(), count, NO_RANDOM))
                    .isInstanceOf(IllegalArgumentException.class);
            assertThatThrownBy(() -> RandomSampler.sampleWithReplacement(Collections.emptyList(), count, NO_RANDOM))
                    .isInstanceOf(IllegalArgumentException.class);
        }
    }

    @Test
    void rejectsNullSourceOrRandomGenerator() {
        Set<String> source = Collections.singleton("a");
        assertThatThrownBy(() -> RandomSampler.sampleDistinct(null, 1, NO_RANDOM))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> RandomSampler.sampleWithReplacement(null, 1, NO_RANDOM))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> RandomSampler.sampleDistinct(source, 1, null))
                .isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> RandomSampler.sampleWithReplacement(source, 1, null))
                .isInstanceOf(NullPointerException.class);
    }

    @Test
    void samplesNullMembers() {
        Set<String> source = new LinkedHashSet<>(Arrays.asList(null, "a", "b"));
        assertThat(RandomSampler.sampleDistinct(source, 3, getRandom()))
                .containsExactlyInAnyOrder(null, "a", "b");
        assertThat(RandomSampler.sampleWithReplacement(source, 100, getRandom()))
                .containsOnly(null, "a", "b");
        assertThat(RandomSampler.sampleWithReplacement(Collections.<String>singleton(null), 3,
                getRandom())).containsExactly(null, null, null);
    }

    private static ThreadLocalRandom getRandom() {
        return ThreadLocalRandom.current();
    }
}
