package ru.mail.polis.kate.moreva;

import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

final class SSTable implements Table {

    private final int size;
    private final int count;
    private final FileChannel fileChannel;
    private static final ByteBuffer intBuffer = ByteBuffer.allocate(Integer.BYTES);
    private static final ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

    SSTable(@NotNull final Path path) throws IOException {
        this.fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        final int fileSize = (int) (fileChannel.size() - Integer.BYTES);
        final ByteBuffer cellByteBuffer = intBuffer.rewind();
        this.fileChannel.read(cellByteBuffer, fileSize);
        this.count = cellByteBuffer.rewind().getInt();
        this.size = fileSize - count * Integer.BYTES;
    }

    static void serialize(@NotNull final File toFile,
                          @NotNull final Iterator<Cell> iterator) throws IOException {
        try (FileChannel file = new FileOutputStream(toFile).getChannel()) {
            final List<Integer> offsets = new ArrayList<>();
            int offset = 0;
            while (iterator.hasNext()) {
                final Cell cell = iterator.next();
                final ByteBuffer key = cell.getKey();
                offsets.add(offset);
                offset += key.remaining() + Long.BYTES + Integer.BYTES;
                file.write(intBuffer.rewind().putInt(key.remaining()).rewind());

                file.write(key);
                if (cell.getValue().isTombstone()) {
                    file.write(longBuffer.rewind()
                            .putLong(-cell.getValue().getTimestamp())
                            .rewind());
                } else {
                    file.write(longBuffer.rewind()
                            .putLong(cell.getValue().getTimestamp())
                            .rewind());

                    final ByteBuffer data = cell.getValue().getData();
                    offset += data.remaining();
                    file.write(data);
                }
            }
            final AtomicReference<IOException> ioe = new AtomicReference<>();
            offsets.forEach(offsetValue -> {
                try {
                    file.write(intBuffer.rewind()
                            .putInt(offsetValue)
                            .rewind());
                } catch (IOException e) {
                    ioe.set(e);
                }
            });
            final IOException ioException = ioe.get();
            if (ioException != null) {
                throw ioException;
            }

            final int count = offsets.size();
            file.write(intBuffer.rewind().putInt(count).rewind());
        }
    }

    @Override
    public void upsert(@NotNull final ByteBuffer key, @NotNull final ByteBuffer value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove(@NotNull final ByteBuffer key) {
        throw new UnsupportedOperationException();
    }

    @NotNull
    @Override
    public Iterator<Cell> iterator(@NotNull final ByteBuffer from) throws IOException {
        return new Iterator<>() {

            int rowPosition = getKeyPosition(from);

            @Override
            public boolean hasNext() {
                return rowPosition < count;
            }

            @Override
            public Cell next() {
                assert hasNext();
                return getCell(rowPosition++);
            }
        };
    }

    @Override
    public long sizeInBytes() {
        return (long) size + (count + 1) * Integer.BYTES;
    }

    private Cell getCell(final int rowPosition) {
        try {
            int offset = getOffset(rowPosition);
            final ByteBuffer keyByteBufferSize = intBuffer.rewind();
            fileChannel.read(keyByteBufferSize, offset);
            offset += Integer.BYTES;

            final int keySize = keyByteBufferSize.rewind().getInt();
            final ByteBuffer keyByteBuffer = ByteBuffer.allocate(keySize);
            fileChannel.read(keyByteBuffer, offset);
            offset += keySize;

            final ByteBuffer timestampBuffer = longBuffer.rewind();
            fileChannel.read(timestampBuffer, offset);
            final long timestamp = timestampBuffer.rewind().getLong();

            if (timestamp < 0) {
                return new Cell(keyByteBuffer.rewind(), new Value(-timestamp));
            } else {
                offset += Long.BYTES;
                final int dataSize;
                if (rowPosition == count - 1) {
                    dataSize = size - offset;
                } else {
                    dataSize = getOffset(rowPosition + 1) - offset;
                }
                final ByteBuffer data = ByteBuffer.allocate(dataSize);
                fileChannel.read(data, offset);
                return new Cell(keyByteBuffer.rewind(), new Value(data.rewind(), timestamp));
            }
        } catch (IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private int getKeyPosition(@NotNull final ByteBuffer from) throws IOException {
        assert count > 0;

        int left = 0;
        int right = count - 1;

        while (left <= right) {
            final int mid = (left + right) / 2;
            final ByteBuffer keyByteBuffer = getKey(mid);
            final int resultCmp = keyByteBuffer.compareTo(from);
            if (resultCmp > 0) {
                right = mid - 1;
            } else if (resultCmp < 0) {
                left = mid + 1;
            } else {
                return mid;
            }
        }
        return left;
    }

    private ByteBuffer getKey(final int rowPosition) throws IOException {
        final ByteBuffer byteBuffer = intBuffer.rewind();
        final int offset = getOffset(rowPosition);

        fileChannel.read(byteBuffer, offset);
        final int size = byteBuffer.rewind().getInt();
        final ByteBuffer keyBuffer = ByteBuffer.allocate(size);
        fileChannel.read(keyBuffer, offset + Integer.BYTES);
        return keyBuffer.rewind();
    }

    private int getOffset(final int rowPosition) throws IOException {
        final ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES);
        fileChannel.read(byteBuffer, size + rowPosition * Integer.BYTES);
        return byteBuffer.rewind().getInt();
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
