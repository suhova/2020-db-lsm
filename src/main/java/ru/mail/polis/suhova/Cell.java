package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Cell implements Comparable<Cell> {
    private final ByteBuffer key;
    private final Value value;

    public Cell(final ByteBuffer key, final Value value) {
        this.key = key;
        this.value = value;
    }

    public static Cell of(
            @NotNull final ByteBuffer key,
            @NotNull final Value value) {
        return new Cell(key, value);
    }

    public ByteBuffer getKey() {
        return key.asReadOnlyBuffer();
    }

    public Value getValue() {
        return value;
    }

    @Override
    public int compareTo(@NotNull Cell cell) {
        final int cmp = key.compareTo(cell.getKey());
        return cmp == 0 ? Long.compare(cell.getValue().getVersion(), value.getVersion()) : cmp;
    }
}
