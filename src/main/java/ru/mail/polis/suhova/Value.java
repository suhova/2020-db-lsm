package ru.mail.polis.suhova;

import java.nio.ByteBuffer;

public class Value {
    private final ByteBuffer data;
    private final boolean tombstone;
    private final long version;

    public Value(ByteBuffer data) {
        this.data = data;
        this.tombstone = false;
        this.version = System.nanoTime();
    }

    public Value(ByteBuffer data, boolean tombstone, long version) {
        this.data = data;
        this.tombstone = tombstone;
        this.version = version;
    }

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
