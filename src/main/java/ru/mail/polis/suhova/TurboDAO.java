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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;

import static java.util.Objects.requireNonNull;

public class TurboDAO implements DAO {
    private static final String SUFFIX = "sst.dat";
    private static final String TEMP = "sst.tmp";
    private final long flushThreshold;
    private final File dir;
    private final NavigableMap<Integer, Table> ssTables = new TreeMap<>();
    private MemTable memTable;
    private int generation;

    /**
     * Implementation {@link DAO}.
     *
     * @param dir            - directory
     * @param flushThreshold - when the table reaches this size, it flushes
     */
    public TurboDAO(@NotNull final File dir, final long flushThreshold) {
        this.memTable = new MemTable();
        this.flushThreshold = flushThreshold;
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
                                ssTables.put(gen, new SSTable(f));
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
        final List<Iterator<Cell>> iters = new ArrayList<>(ssTables.size() + 1);
        iters.add(memTable.iterator(from));
        ssTables.descendingMap().values().forEach(table -> iters.add(table.iterator(from)));
        final Iterator<Cell> merged = Iterators.mergeSorted(iters, Comparator.naturalOrder());
        final Iterator<Cell> fresh = Iters.collapseEquals(merged, Cell::getKey);
        final Iterator<Cell> alive = Iterators.filter(fresh, cell -> !requireNonNull(cell).getValue().isTombstone());
        return Iterators.transform(alive, cell -> Record.of(requireNonNull(cell).getKey(), cell.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush();
        }
        memTable.upsert(key, value);
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (memTable.sizeInBytes() >= flushThreshold) {
            flush();
        }
        memTable.remove(key);
    }

    @Override
    public void close() throws IOException {
        if (memTable.getEntryCount() > 0) {
            flush();
        }
        ssTables.values().forEach(Table::close);
    }

    private void flush() throws IOException {
        final File tmp = new File(dir, generation + TEMP);
        SSTable.write(tmp, memTable.iterator(ByteBuffer.allocate(0)));
        final File dat = new File(dir, generation + SUFFIX);
        Files.move(tmp.toPath(), dat.toPath(), StandardCopyOption.ATOMIC_MOVE);
        ssTables.put(generation, new SSTable(dat));
        generation++;
        memTable = new MemTable();
    }
}
