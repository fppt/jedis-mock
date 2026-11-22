package com.github.fppt.jedismock.datastructures;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;

class RandomSamplerTest {
    @Test
    void samplesEdgeCases() {
        Set<Integer> source = IntStream.range(0, 1000).boxed()
                .collect(Collectors.toCollection(LinkedHashSet::new));
        Set<Integer> before = new LinkedHashSet<>(source);
        for (int count : new int[]{1, 3, 999, 1000, 1001}) {
            List<Integer> result = RandomSampler.sampleDistinct(source, count, new Random(42));
            assertThat(result).hasSize(Math.min(count, source.size())).doesNotHaveDuplicates();
            assertThat(source).containsAll(result);
        }
        assertThat(source).containsExactlyElementsOf(before);
    }

    @Test
    void supportsMoreDrawsThanMembersWithReplacement() {
        Set<String> source = new LinkedHashSet<>(Arrays.asList("a", "b", "c"));
        List<String> result = RandomSampler.sampleWithReplacement(source, 1000, new Random(42));
        assertThat(result).hasSize(1000).containsOnly("a", "b", "c");
        assertThat(result.subList(0, 100)).contains("a", "b", "c");
        assertThat(source).containsExactly("a", "b", "c");
        assertThat(RandomSampler.sampleWithReplacement(Collections.singleton("a"), 3, new Random(42)))
                .containsExactly("a", "a", "a");
    }

    @Test
    void resetsTheProvidedGeneratorForReplay() {
        Set<String> members = new LinkedHashSet<>(Arrays.asList("a", "b", "c", "d", "e"));
        Random random = new Random(42);
        List<String> distinct = RandomSampler.sampleDistinct(members, 4, random);
        List<String> repeated = RandomSampler.sampleWithReplacement(members, 50, random);

        random.setSeed(42);
        assertThat(RandomSampler.sampleDistinct(members, 4, random)).containsExactlyElementsOf(distinct);
        assertThat(RandomSampler.sampleWithReplacement(members, 50, random)).containsExactlyElementsOf(repeated);
    }

    @Test
    void returnsEmptyForEmptySourcesAndNonPositiveCounts() {
        Set<String> source = Collections.singleton("a");
        for (int count : new int[]{0, -1, Integer.MIN_VALUE}) {
            assertThat(RandomSampler.sampleDistinct(source, count, new Random(42))).isEmpty();
            assertThat(RandomSampler.sampleWithReplacement(source, count, new Random(42))).isEmpty();
        }
        assertThat(RandomSampler.sampleDistinct(Collections.emptySet(), 3, new Random(42))).isEmpty();
        assertThat(RandomSampler.sampleWithReplacement(Collections.emptySet(), 3, new Random(42))).isEmpty();
    }

    @Test
    void samplesNullMembers() {
        Set<String> source = new LinkedHashSet<>(Arrays.asList(null, "a", "b"));
        assertThat(RandomSampler.sampleDistinct(source, 3, new Random(42)))
                .containsExactlyInAnyOrder(null, "a", "b");
        assertThat(RandomSampler.sampleWithReplacement(source, 100, new Random(42)))
                .containsOnly(null, "a", "b").containsNull();
    }
}
