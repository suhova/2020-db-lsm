package ru.mail.polis.suhova;

import java.nio.ByteBuffer;

public class Value {
    private final ByteBuffer data;
    private final boolean tombstone;
    private final long version;

    /**
     * Value from {@link TableEntry}
     * @param data - content
     */
    public Value(final ByteBuffer data) {
        this.data = data;
        this.tombstone = false;
        this.version = System.nanoTime();
    }
    /**
     * Value from {@link TableEntry}
     * @param data - content
     * @param tombstone - is it removed
     * @param version - timestamp
     */
    public Value(final ByteBuffer data, final boolean tombstone, final long version) {
        this.data = data;
        this.tombstone = tombstone;
        this.version = version;
    }
    /**
     * Value from {@link TableEntry}
     */
    public Value() {
        this.data = null;
        this.tombstone = true;
        this.version = System.nanoTime();
    }

    public ByteBuffer getData() {
        return data;
    }

    public boolean isTombstone() {
        return tombstone;
    }

    public long getVersion() {
        return version;
    }
}
