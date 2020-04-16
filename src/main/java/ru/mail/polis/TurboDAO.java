package ru.mail.polis;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;

import java.nio.ByteBuffer;
import java.util.*;

public class TurboDAO implements DAO {
    private final SortedMap<ByteBuffer, ByteBuffer> map = new TreeMap<>();

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(
                map.tailMap(from).entrySet().iterator(),
                e -> Record.of(Objects.requireNonNull(e).getKey(), e.getValue()));
}

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull ByteBuffer value) {
        map.put(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        map.remove(key);
    }

    @Override
    public void close() {
    }
}
