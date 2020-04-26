package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Iterator;

public interface Table {
    @NotNull
    Iterator<TableEntry> iterator(@NotNull ByteBuffer from);

    boolean upsert(@NotNull ByteBuffer key, @NotNull ByteBuffer value);

    boolean remove(@NotNull ByteBuffer key) throws IOException;
}
