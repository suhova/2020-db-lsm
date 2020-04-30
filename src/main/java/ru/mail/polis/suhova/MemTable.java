package ru.mail.polis.suhova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.TreeMap;

public class MemTable implements Table {
    private final NavigableMap<ByteBuffer, Value> map = new TreeMap<>();
    private final long maxSize;
    private long size;

    public MemTable(final long maxSize) {
        this.maxSize = maxSize;
        this.size = 0;
    }

    public int getEntryCount() {
        return map.size();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> Cell.of(Objects.requireNonNull(e).getKey(), e.getValue()));
    }

    @Override
    public boolean upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        map.put(key.duplicate(), new Value(value.duplicate(), System.nanoTime()));
        size += key.remaining() + value.remaining() + Long.BYTES;
        return size <= maxSize;
    }

    @Override
    public boolean remove(@NotNull final ByteBuffer key) {
        if (map.containsKey(key)) {
            if (!map.get(key).isTombstone()) {
                size = size - map.get(key).getData().remaining();
            }
        } else {
            size += key.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.nanoTime()));
        return size <= maxSize;
    }
}
