package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SSTable implements Table {
    private final int count;
    private final IntBuffer offsets;
    private final ByteBuffer cells;
    private int size;

    SSTable(@NotNull final Path file) throws IOException {
        try (FileChannel fileChannel = FileChannel.open(file, StandardOpenOption.READ)) {
            this.size = (int) fileChannel.size();
            final ByteBuffer bb = ByteBuffer.allocate(size);
            for (int i = 0; i < size; i++) {
                fileChannel.read(bb);
            }
            bb.rewind();
            this.count = bb.getInt(size - Integer.BYTES);
            int offset = size - Integer.BYTES * (count + 1);
            bb.position(offset);
            cells = bb.duplicate()
                    .position(0)
                    .limit(size - Integer.BYTES * (count + 1))
                    .slice();
            offsets = bb.duplicate()
                    .position(size - Integer.BYTES * (count + 1))
                    .limit(size - Integer.BYTES)
                    .slice()
                    .asIntBuffer();
            this.size = offset;
        }
    }

    /**
     * Writing a table to a file.
     * keySize (integer)| key | version (long) | data
     * offsets
     * n
     */
    public static void write(final File fileTable, final Iterator<Cell> iter) throws IOException {
        try (FileChannel file = new FileOutputStream(fileTable).getChannel()) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (iter.hasNext()) {
                final Cell cell = iter.next();
                final ByteBuffer key = cell.getKey();
                offsets.add(offset);
                offset += key.remaining() + Long.BYTES + Integer.BYTES;
                file.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(key.remaining())
                        .rewind());
                file.write(key);
                if (cell.getValue().isTombstone()) {
                    file.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(-cell.getValue().getVersion())
                            .rewind());
                } else {
                    file.write(ByteBuffer.allocate(Long.BYTES)
                            .putLong(cell.getValue().getVersion())
                            .rewind());
                    final ByteBuffer data = cell.getValue().getData();
                    offset += data.remaining();
                    file.write(data);
                }
            }
            final int count = offsets.size();
            for (final Integer integer : offsets) {
                file.write(ByteBuffer.allocate(Integer.BYTES)
                        .putInt(integer)
                        .rewind());
            }
            file.write(ByteBuffer.allocate(Integer.BYTES)
                    .putInt(count)
                    .rewind());
        }
    }

    private Cell getCell(final int num) {
        final int keySize = cells.getInt(offsets.get(num));
        int offset = offsets.get(num) + Integer.BYTES;
        final ByteBuffer key = cells.duplicate()
                .position(offset)
                .limit(offset + keySize)
                .slice();
        final long version = cells.getLong(offset + keySize);
        if (version < 0) {
            return new Cell(key, new Value(-version));
        } else {
            offset += keySize + Long.BYTES;
            int lim;
            if (num == this.count - 1) {
                lim = this.size;
            } else {
                lim = offsets.get(num + 1);
            }
            final ByteBuffer data = cells.duplicate()
                    .position(offset)
                    .limit(lim)
                    .slice();
            return new Cell(key, new Value(data, version));
        }
    }

    private ByteBuffer getKey(final int num) {
        final int keySize = cells.getInt(offsets.get(num));
        final int offset = offsets.get(num) + Integer.BYTES;
        return cells.duplicate()
                .position(offset)
                .limit(offset + keySize)
                .slice();
    }

    private int getKeyPosition(final ByteBuffer key) {
        int low = 0;
        int high = count - 1;
        while (low <= high) {
            final int mid = low + (high - low) / 2;
            final int cmp = getKey(mid).compareTo(key);
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

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) {
        return new Iterator<>() {
            int position = getKeyPosition(from);

            @Override
            public boolean hasNext() {
                return position < count;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return getCell(position++);
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
