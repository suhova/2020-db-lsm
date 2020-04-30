package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;

public class Value {
    private final ByteBuffer data;
    private final long version;

    /**
     * Value from {@link Cell}.
     *
     * @param data    - content
     * @param version - timestamp
     */
    public Value(@NotNull final ByteBuffer data, final long version) {
        this.data = data;
        this.version = version;
    }

    /**
     * new tombstone.
     */
    public Value(final long version) {
        this.data = null;
        this.version = version;
    }

    public ByteBuffer getData() {
        return data;
    }

    public boolean isTombstone() {
        return data == null;
    }

    public long getVersion() {
        return version;
    }
}
