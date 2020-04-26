package ru.mail.polis.suhova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.Objects;
import java.util.SortedMap;
import java.util.TreeMap;

public class MemTable implements Table {
    private static final int ADDINTIONAL_SIZE = Long.BYTES + Byte.BYTES;
    private final SortedMap<ByteBuffer, Value> map = new TreeMap<>();
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
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> TableEntry.of(Objects.requireNonNull(e).getKey(), e.getValue()));
    }

    public Iterator<TableEntry> iterator() {
        return Iterators.transform(
                map.entrySet().iterator(),
                e -> TableEntry.of(Objects.requireNonNull(e).getKey(), e.getValue()));
    }

    @Override
    public boolean upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        size += key.remaining() + value.remaining() + ADDINTIONAL_SIZE;
        if (size <= maxSize) {
            map.put(key, new Value(value.duplicate()));
            return true;
        } else {
            size = size - key.remaining() - value.remaining() - ADDINTIONAL_SIZE;
            return false;
        }
    }

    @Override
    public boolean remove(@NotNull final ByteBuffer key) {
        if (map.containsKey(key)) {
            if (!map.get(key).isTombstone()) {
                size = size - map.get(key).getData().remaining();
            }
        } else {
            size += key.remaining() + ADDINTIONAL_SIZE;
        }
        if (size <= maxSize) {
            map.put(key, new Value());
        } else {
            size = size - key.remaining() - ADDINTIONAL_SIZE;
            return false;
        }
        return true;
    }
}
