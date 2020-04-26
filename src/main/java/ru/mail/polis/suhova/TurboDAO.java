package ru.mail.polis.suhova;

import com.google.common.collect.Iterators;
import org.jetbrains.annotations.NotNull;
import ru.mail.polis.DAO;
import ru.mail.polis.Iters;
import ru.mail.polis.Record;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class TurboDAO implements DAO {
    private final long maxSize;
    private final File dir;
    private final ArrayList<Table> sstables;
    private MemTable memTable;
    private int generation;

    /**
     * Implementation {@link DAO}.
     * @param dir - directory
     * @param maxSize - maximum size in bytes
     */
    public TurboDAO(final File dir, final long maxSize) {
        this.memTable = new MemTable(maxSize);
        this.maxSize = maxSize;
        this.dir = dir;
        this.sstables = new ArrayList<>();
        final File[] sst = dir.listFiles();
        if (sst == null) {
            generation = 0;
        } else {
            generation = sst.length;
            for (final File file : sst) {
                if(file.getName().endsWith("sst.txt")) {
                    sstables.add(new SSTable(file));
                }
            }
        }
    }

    @NotNull
    @Override
    public Iterator<Record> iterator(@NotNull final ByteBuffer from) {
        return Iterators.transform(Iterators.filter(
                Iters.collapseEquals(Iterators.mergeSorted(Stream.concat(sstables.stream(), Stream.of(memTable))
                        .map(s -> s.iterator(from))
                        .collect(Collectors.toList()), Comparator.naturalOrder()), TableEntry::getKey),
                e -> !Objects.requireNonNull(e).getValue().isTombstone()),
                tableEntry -> Record.of(Objects.requireNonNull(tableEntry).getKey(), tableEntry.getValue().getData()));
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) throws IOException {
        if (!memTable.upsert(key, value)) {
            sstables.add(new SSTable(dir, generation, memTable.iterator()));
            generation++;
            memTable = new MemTable(maxSize);
            if (!memTable.upsert(key, value)) {
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) throws IOException {
        if (!memTable.remove(key)) {
            sstables.add(new SSTable(dir, generation, memTable.iterator()));
            generation++;
            memTable = new MemTable(maxSize);
            if (!memTable.remove(key)) {
                throw new IllegalArgumentException();
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (memTable.getEntryCount() != 0) {
            new SSTable(dir, generation, memTable.iterator());
        }
    }
}
