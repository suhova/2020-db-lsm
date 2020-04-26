package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SSTable implements Table {
    private final File fileTable;
    private final int size;
    private final int n;
    private final List<Integer> offsets = new ArrayList<>();
    private final List<Integer> keySizes = new ArrayList<>();

    SSTable(final File file) throws IOException {
        if (file.isDirectory() || !file.exists()) {
            throw new IOException();
        }
        this.fileTable = file;
        try (FileChannel fileChannel = FileChannel.open(file.toPath(), StandardOpenOption.READ)) {
            this.size = (int) fileChannel.size();
            ByteBuffer bb = ByteBuffer.allocate(size);
            for (int i = 0; i < size; i++) {
                fileChannel.read(bb);
            }
            bb.rewind();
            this.n = bb.getInt(size - Integer.BYTES);
            int offset = size - Integer.BYTES * (n * 2 + 1);
            bb.position(offset);
            for (int i = 0; i < this.n; i++) {
                offsets.add(bb.getInt());
                offset += Integer.BYTES;
                bb.position(offset);
                keySizes.add(bb.getInt());
                offset += Integer.BYTES;
                bb.position(offset);
            }
        }
    }

    /*
     * version (long) | tombstone (byte) | key | data
     * offset|keySizes
     * n
     */
    public SSTable(final File dir, int generation, final Iterator<TableEntry> iter) throws IOException {
        this.fileTable = createFile(dir, generation);
        try (FileChannel file = new FileOutputStream(fileTable).getChannel()) {
            int offset = 0;
            while (iter.hasNext()) {
                TableEntry entry = iter.next();
                ByteBuffer key = entry.getKey();
                this.offsets.add(offset);
                this.keySizes.add(key.remaining());
                offset += key.remaining() + Long.BYTES + Byte.BYTES;
                file.write(ByteBuffer.allocate(Long.BYTES)
                        .putLong(entry.getValue().getVersion())
                        .rewind());
                if (entry.getValue().isTombstone()) {
                    file.write(ByteBuffer.allocate(1)
                            .put((byte) 1)
                            .rewind());
                    file.write(key);
                } else {
                    file.write(ByteBuffer.allocate(1)
                            .put((byte) 0)
                            .rewind());
                    ByteBuffer data = entry.getValue().getData();
                    offset += data.remaining();
                    file.write(key);
                    file.write(data);
                }
            }
            this.n = offsets.size();
            this.size = offset + Integer.BYTES * (n * 2 + 1);
            for (int i = 0; i < this.n; i++) {
                file.write(ByteBuffer.allocate(Integer.BYTES * 2)
                        .putInt(offsets.get(i))
                        .putInt(keySizes.get(i))
                        .rewind());
            }
            file.write(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(offsets.size())
                    .rewind());
        }
    }

    private TableEntry getTableEntry(final int num) {
        try (FileChannel fileChannel = FileChannel.open(this.fileTable.toPath(), StandardOpenOption.READ)) {
            ByteBuffer bb = ByteBuffer.allocate(size);
            for (int i = 0; i < size; i++) {
                fileChannel.read(bb);
            }
            bb.rewind();
            final long version = bb.getLong(offsets.get(num));
            final boolean tombstone = bb.get(offsets.get(num) + Long.BYTES) == 1;
            int offset = offsets.get(num) + Long.BYTES + Byte.BYTES;
            final ByteBuffer key = bb.duplicate()
                    .position(offset)
                    .limit(offset + keySizes.get(num))
                    .slice();
            if (!tombstone) {
                offset += keySizes.get(num);
                int lim;
                if (num == this.n - 1) {
                    lim = this.size - Integer.BYTES * (n * 2 + 1);
                } else {
                    lim = offsets.get(num + 1);
                }
                final ByteBuffer data = bb.duplicate()
                        .position(offset)
                        .limit(lim)
                        .slice();
                return new TableEntry(key, new Value(data, false, version));
            } else {
                return new TableEntry(key, new Value(null, true, version));
            }
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private int getKeyPosition(ByteBuffer key) {
        int low = 0;
        int high = n - 1;
        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final int cmp = getTableEntry(mid).getKey().compareTo(key);
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                return mid;
            }
        }
        return low;
    }

    private File createFile(final File dir, final int generation) throws IOException {
        if (!dir.isDirectory()) {
            throw new IOException();
        }
        final File file = new File(dir.getPath(), generation + "sst.txt");
        if (!file.createNewFile()) {
            throw new IOException();
        }
        return file;
    }

    @NotNull
    @Override
    public Iterator<TableEntry> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int position = getKeyPosition(from);

            @Override
            public boolean hasNext() {
                return position < n;
            }

            @Override
            public TableEntry next() {
                assert hasNext();
                return getTableEntry(position++);
            }
        };
    }

    @Override
    public boolean upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException();
    }
}
