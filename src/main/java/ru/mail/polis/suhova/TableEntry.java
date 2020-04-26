package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.Comparator;

public class TableEntry implements Comparable<TableEntry> {
    private final ByteBuffer key;
    private final Value value;

    public TableEntry(ByteBuffer key, Value value) {
        this.key = key;
        this.value = value;
    }

    public static TableEntry of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        return new TableEntry(key, value);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull TableEntry entry) {
        int cmp = key.compareTo(entry.getKey());
        return cmp == 0 ? -Long.compare(value.getVersion(), entry.getValue().getVersion()) : cmp;
    }
}
