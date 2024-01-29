package com.giraone.streaming.service;

import reactor.core.publisher.Mono;
import reactor.core.publisher.MonoSink;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousByteChannel;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.Objects;

/**
 * Utility methods for IO streaming tasks on channels
 * ({@link WritableByteChannel}, {@link ReadableByteChannel}, {@link AsynchronousByteChannel}), like
 * <ul>
 *     <li>writing to channels</li>
 *     <li>>reading from channels</li>
 *     <li>>transferring between channels</li>
 * </ul
 */
public final class IoChannelUtils {

    private static final int DEFAULT_BUFFER_SIZE = 8192;
    private static final int SIXTY_FOUR_KB = 64 * 1024;
    private static final int THIRTY_TWO_KB = 32 * 1024;
    private static final int MB = 1024 * 1024;
    private static final int GB = 1024 * MB;

    // Hide
    private IoChannelUtils() {
    }

    /**
     * Adapts {@link AsynchronousFileChannel} to {@link AsynchronousByteChannel}.
     * @param fileChannel The {@link AsynchronousFileChannel}.
     * @param position The position in the file to begin writing or reading the {@code content}.
     * @return A {@link AsynchronousByteChannel} that delegates to {@code fileChannel}.
     * @throws NullPointerException When {@code fileChannel} is null.
     * @throws IllegalArgumentException When {@code position} is negative.
     */
    public static AsynchronousByteChannel toAsynchronousByteChannel(AsynchronousFileChannel fileChannel,                                                                   long position) {
        Objects.requireNonNull(fileChannel, "'fileChannel' must not be null");
        if (position < 0) {
            throw new IllegalArgumentException("'position' cannot be less than 0.");
        }
        return new AsynchronousFileChannelAdapter(fileChannel, position);
    }

    /**
     * Transfers bytes from {@link ReadableByteChannel} to {@link WritableByteChannel}.
     *
     * @param source A source {@link ReadableByteChannel}.
     * @param destination A destination {@link WritableByteChannel}.
     * @throws IOException When I/O operation fails.
     * @throws NullPointerException When {@code source} or {@code destination} is null.
     */
    public static void transfer(ReadableByteChannel source, WritableByteChannel destination) throws IOException {
        transfer(source, destination, null);
    }

    /**
     * Transfers bytes from {@link ReadableByteChannel} to {@link WritableByteChannel}.
     *
     * @param source A source {@link ReadableByteChannel}.
     * @param destination A destination {@link WritableByteChannel}.
     * @param estimatedSourceSize An estimated size of the source channel, may be null. Used to better determine the
     * size of the buffer used to transfer data in an attempt to reduce read and write calls.
     * @throws IOException When I/O operation fails.
     * @throws NullPointerException When {@code source} or {@code destination} is null.
     */
    public static void transfer(ReadableByteChannel source, WritableByteChannel destination, Long estimatedSourceSize)
        throws IOException {
        if (source == null && destination == null) {
            throw new NullPointerException("'source' and 'destination' cannot be null.");
        } else if (source == null) {
            throw new NullPointerException("'source' cannot be null.");
        } else if (destination == null) {
            throw new NullPointerException("'destination' cannot be null.");
        }

        int bufferSize = (estimatedSourceSize == null) ? getBufferSize(source) : getBufferSize(estimatedSourceSize);
        ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
        int read;
        do {
            buffer.clear();
            read = source.read(buffer);
            buffer.flip();
            fullyWriteBuffer(buffer, destination);
        } while (read >= 0);
    }

    /**
     * Transfers bytes from {@link ReadableByteChannel} to {@link AsynchronousByteChannel}.
     *
     * @param source A source {@link ReadableByteChannel}.
     * @param destination A destination {@link AsynchronousByteChannel}.
     * @return A {@link Mono} that completes when transfer is finished.
     * @throws NullPointerException When {@code source} or {@code destination} is null.
     */
    public static Mono<Void> transferAsync(ReadableByteChannel source, AsynchronousByteChannel destination) {
        return transferAsync(source, destination, null);
    }

    /**
     * Transfers bytes from {@link ReadableByteChannel} to {@link AsynchronousByteChannel}.
     *
     * @param source A source {@link ReadableByteChannel}.
     * @param destination A destination {@link AsynchronousByteChannel}.
     * @param estimatedSourceSize An estimated size of the source channel, may be null. Used to better determine the
     * size of the buffer used to transfer data in an attempt to reduce read and write calls.
     * @return A {@link Mono} that completes when transfer is finished.
     * @throws NullPointerException When {@code source} or {@code destination} is null.
     */
    public static Mono<Void> transferAsync(ReadableByteChannel source, AsynchronousByteChannel destination,
                                           Long estimatedSourceSize) {
        if (source == null && destination == null) {
            return Mono.error(new NullPointerException("'source' and 'destination' cannot be null."));
        } else if (source == null) {
            return Mono.error(new NullPointerException("'source' cannot be null."));
        } else if (destination == null) {
            return Mono.error(new NullPointerException("'destination' cannot be null."));
        }

        int bufferSize = (estimatedSourceSize == null) ? getBufferSize(source) : getBufferSize(estimatedSourceSize);
        return Mono.create(sink -> sink.onRequest(value -> {
            ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
            try {
                transferAsynchronously(source, destination, buffer, sink);
            } catch (IOException e) {
                sink.error(e);
            }
        }));
    }

    private static void transferAsynchronously(ReadableByteChannel source, AsynchronousByteChannel destination,
                                               ByteBuffer buffer, MonoSink<Void> sink) throws IOException {
        buffer.clear();
        int read = source.read(buffer);
        if (read >= 0) {
            buffer.flip();
            destination.write(buffer, buffer, new CompletionHandler<Integer, ByteBuffer>() {
                @Override
                public void completed(Integer result, ByteBuffer attachment) {
                    try {
                        // This is not a classic recursion.
                        // I.e. it happens in completion handler not on call stack.
                        if (buffer.hasRemaining()) {
                            destination.write(buffer, buffer, this);
                        } else {
                            transferAsynchronously(source, destination, buffer, sink);
                        }
                    } catch (IOException e) {
                        sink.error(e);
                    }
                }

                @Override
                public void failed(Throwable e, ByteBuffer attachment) {
                    sink.error(e);
                }
            });
        } else {
            sink.success();
        }
    }

    /**
     * Fully writes a {@link ByteBuffer} to a {@link WritableByteChannel}.
     * <p>
     * This handles scenarios where write operations don't write the entirety of the {@link ByteBuffer} in a single
     * call.
     *
     * @param buffer The {@link ByteBuffer} to write.
     * @param channel The {@link WritableByteChannel} to write the {@code buffer} to.
     * @throws IOException If an I/O error occurs while writing to the {@code channel}.
     */
    public static void fullyWriteBuffer(ByteBuffer buffer, WritableByteChannel channel) throws IOException {
        while (buffer.hasRemaining()) {
            channel.write(buffer);
        }
    }

    //------------------------------------------------------------------------------------------------------------------

    /*
     * Helper method to optimize the size of the read buffer to reduce the number of reads and writes that have to be
     * performed. If the source and/or target is IO-based, file, network connection, etc, this reduces the number of
     * calls that may have to be handled by the system.
     */
    private static int getBufferSize(ReadableByteChannel source) {
        if (!(source instanceof SeekableByteChannel)) {
            return DEFAULT_BUFFER_SIZE;
        }

        SeekableByteChannel seekableSource = (SeekableByteChannel) source;
        try {
            long size = seekableSource.size();
            long position = seekableSource.position();
            return getBufferSize(size - position);
        } catch (IOException ex) {
            // Don't let an IOException prevent transfer when we are only trying to gain information.
            return DEFAULT_BUFFER_SIZE;
        }
    }

    private static int getBufferSize(long dataSize) {
        if (dataSize > GB) {
            return SIXTY_FOUR_KB;
        } else if (dataSize > MB) {
            return THIRTY_TWO_KB;
        } else {
            return DEFAULT_BUFFER_SIZE;
        }
    }
}
