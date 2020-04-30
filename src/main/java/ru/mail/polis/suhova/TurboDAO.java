package ru.mail.polis.suhova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.Objects;
import java.util.TreeMap;
import java.util.Iterator;
import java.util.Comparator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TurboDAO implements DAO {
    private static final String SUFFIX = "sst.dat";
    private static final String TEMP = "sst.tmp";
    private final long maxSize;
    private final File dir;
    private final TreeMap<Integer, Table> ssTables = new TreeMap<>();
    private MemTable memTable;
    private int generation;

    /**
     * Implementation {@link DAO}.
     *
     * @param dir     - directory
     * @param maxSize - maximum size in bytes
     */
    public TurboDAO(@NotNull final File dir, final long maxSize) {
        this.memTable = new MemTable(maxSize);
        this.maxSize = maxSize;
        this.dir = dir;
        generation = -1;
        final File[] list = dir.listFiles((dir1, name) -> name.endsWith(SUFFIX));
        assert list != null;
        Arrays.stream(list)
                .filter(file -> !file.isDirectory())
                .forEach(
                        f -> {
                            final String name = f.getName();
                            final int gen = Integer.parseInt(name.substring(0, name.indexOf(SUFFIX)));
                            try {
                                ssTables.put(gen, new SSTable(f.toPath()));
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                            if (gen > generation) generation = gen;
                        }
                );
        generation++;
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(Iterators.filter(
                Iters.collapseEquals(Iterators.mergeSorted(
                        Stream.concat(ssTables.descendingMap().values().stream(),
                                Stream.of(memTable))
                                .map(s -> s.iterator(from))
                                .collect(Collectors.toList()), Comparator.naturalOrder()), Cell::getKey),
                e -> !Objects.requireNonNull(e).getValue().isTombstone()),
                tableEntry -> Record.of(Objects.requireNonNull(tableEntry).getKey(), tableEntry.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (!memTable.upsert(key, value)) {
            flush();
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (!memTable.remove(key)) {
            flush();
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.getEntryCount() > 0) {
            flush();
        }
    }

    private void flush() throws IOException {
        final File tmp = new File(dir, generation + TEMP);
        SSTable.write(tmp, memTable.iterator(ByteBuffer.allocate(0)));
        final File dat = new File(dir, generation + SUFFIX);
        Files.move(tmp.toPath(), dat.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables.put(generation, new SSTable(dat.toPath()));
        generation++;
        memTable = new MemTable(maxSize);
    }
}
