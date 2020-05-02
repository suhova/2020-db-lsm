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
    private long size;

    public MemTable() {
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
                e -> new Cell(Objects.requireNonNull(e).getKey(), e.getValue()));
    }

    @Override
    public void  upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        map.put(key.duplicate(), new Value(value.duplicate(), System.currentTimeMillis()));
        size += key.remaining() + value.remaining() + Long.BYTES;
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        if (map.containsKey(key)) {
            if (!map.get(key).isTombstone()) {
                size = size - map.get(key).getData().remaining();
            }
        } else {
            size += key.remaining() + Long.BYTES;
        }
        map.put(key, new Value(System.currentTimeMillis()));
    }

    @Override
    public long sizeInBytes() {
        return size;
    }
}
