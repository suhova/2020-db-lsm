package ru.mail.polis.suhova;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public final class SSTable implements Table {
    private final IntBuffer offsets;
    private final int count;
    private final int size;
    private final RandomAccessFile randomAccessFile;

    SSTable(@NotNull final File file) throws IOException {
        randomAccessFile = new RandomAccessFile(file, "r");
        final int fileSize = (int) randomAccessFile.getChannel().size();
        randomAccessFile.seek(fileSize - Integer.BYTES);
        this.count = randomAccessFile.readInt();
        this.size = fileSize - Integer.BYTES * (count + 1);
        randomAccessFile.seek(this.size);
        final byte[] bytes = new byte[Integer.BYTES * count];
        randomAccessFile.read(bytes);
        offsets = ByteBuffer.wrap(bytes).asIntBuffer();
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
        try {
            final int keySize = randomAccessFile.readInt();
            final byte[] keyBytes = new byte[keySize];
            randomAccessFile.read(keyBytes);
            final ByteBuffer key = ByteBuffer.wrap(keyBytes);
            final long version = randomAccessFile.readLong();
            if (version < 0) {
                return new Cell(key, new Value(-version));
            } else {
                final int offset = offsets.get(num) + Integer.BYTES + keySize + Long.BYTES;
                int lim;
                if (num == this.count - 1) {
                    lim = this.size - offset;
                } else {
                    lim = offsets.get(num + 1) - offset;
                }
                final byte[] dataBytes = new byte[lim];
                randomAccessFile.read(dataBytes);
                return new Cell(key, new Value(ByteBuffer.wrap(dataBytes), version));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private ByteBuffer getKey(final int num) throws IOException {
        randomAccessFile.seek(offsets.get(num));
        final int keySize = randomAccessFile.readInt();
        final int offset = offsets.get(num) + Integer.BYTES;
        final byte[] keyBytes = new byte[keySize];
        randomAccessFile.seek(offset);
        randomAccessFile.read(keyBytes);
        return ByteBuffer.wrap(keyBytes);
    }

    private int getKeyPosition(final ByteBuffer key) {
        int low = 0;
        int high = count - 1;
        while (low <= high) {
            final int mid = low + (high - low) / 2;
            int cmp;
            try {
                cmp = getKey(mid).compareTo(key);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            if (cmp < 0) {
                low = mid + 1;
            } else if (cmp > 0) {
                high = mid - 1;
            } else {
                try {
                    randomAccessFile.seek(offsets.get(mid));
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return mid;
            }
        }
        try {
            if (low < count) {
                randomAccessFile.seek(offsets.get(low));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sizeInBytes() {
        return size;
    }

    @Override
    public void close() {
        try {
            randomAccessFile.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
