package com.github.fppt.jedismock.datastructures;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.Set;

import static com.github.fppt.jedismock.datastructures.Slice.create;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class IndexedSetTest {

    @Test
    void addSearchAndDelete() {
        IndexedSet<Slice> set = new IndexedSet<>();
        assertThat(set.add(create("a"))).isTrue();
        assertThat(set.add(create("b"))).isTrue();
        assertThat(set.add(create("c"))).isTrue();
        assertThat(set.add(create("d"))).isTrue();
        assertThat(set.add(create("a"))).isFalse();
        assertThat(set).hasSize(4);

        assertThat(set.contains(create("c"))).isTrue();
        assertThat(set.contains(create("z"))).isFalse();

        // Removing "a" frees slot 0 and moves the last element "d" into it,
        // so "d" has to be found and deleted by value at its new slot.
        assertThat(set.remove(create("a"))).isTrue();
        assertThat(set.contains(create("d"))).isTrue();
        assertThat(set.remove(create("d"))).isTrue();
        assertThat(set.remove(create("d"))).isFalse();
        assertThat(set.remove(create("z"))).isFalse();
        assertThat(set).containsExactlyInAnyOrder(create("b"), create("c"));

        // Removal through the iterator: removing "c" from slot 0 moves "f"
        // into that slot, and the iterator must still visit and remove it.
        set.add(create("e"));
        set.add(create("f"));
        set.removeIf(e -> e.equals(create("c")) || e.equals(create("f")));
        assertThat(set.contains(create("c"))).isFalse();
        assertThat(set.contains(create("f"))).isFalse();
        assertThat(set).containsExactlyInAnyOrder(create("b"), create("e"));
    }

    @Test
    void behavesLikeHashSetUnderRandomOperations() {
        Random random = new Random(42);
        IndexedSet<Slice> set = new IndexedSet<>();
        Set<Slice> expected = new HashSet<>();
        for (int i = 0; i < 100_000; i++) {
            Slice value = create(Integer.toString(random.nextInt(50)));
            switch (random.nextInt(4)) {
                case 0, 1 -> assertThat(set.add(value)).isEqualTo(expected.add(value));
                case 2 -> assertThat(set.remove(value)).isEqualTo(expected.remove(value));
                default -> assertThat(set.contains(value)).isEqualTo(expected.contains(value));
            }
            assertThat(set).hasSameSizeAs(expected);
        }
        assertThat(set).containsExactlyInAnyOrderElementsOf(expected);
    }

    @Test
    void randomSelection() {
        Random random = new Random(7);
        IndexedSet<Slice> set = new IndexedSet<>();
        assertThatThrownBy(() -> set.random(random)).isInstanceOf(NoSuchElementException.class);

        for (int i = 0; i < 10; i++) {
            set.add(create(Integer.toString(i)));
        }
        set.remove(create("0"));
        List<Slice> all = new ArrayList<>(set);

        Set<Slice> picked = new HashSet<>();
        for (int i = 0; i < 1000; i++) {
            picked.add(set.random(random));
        }
        assertThat(picked).containsExactlyInAnyOrderElementsOf(all);

        List<Slice> sample = set.sampleDistinct(5, random);
        assertThat(sample).hasSize(5).doesNotHaveDuplicates();
        assertThat(all).containsAll(sample);
        assertThat(set.sampleDistinct(100, random)).containsExactlyInAnyOrderElementsOf(all);
        assertThat(set.sampleWithReplacement(20, random)).hasSize(20).allMatch(set::contains);

        List<Slice> popped = new ArrayList<>();
        while (!set.isEmpty()) {
            popped.add(set.popRandom(random));
        }
        assertThat(popped).containsExactlyInAnyOrderElementsOf(all);
    }
}
