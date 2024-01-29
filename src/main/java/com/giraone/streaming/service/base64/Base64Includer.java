package com.giraone.streaming.service.base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;

public class Base64Includer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Base64Includer.class);

    public static final String CONTENT1 = "<base64-01>";

    private final Tuple2<Flux<ByteBuffer>, Flux<ByteBuffer>> streamTuple;

    public Base64Includer(String json) {
        this.streamTuple = splitToFlux(json);
    }

    public Flux<ByteBuffer> streamWithContent(Flux<ByteBuffer> content) {
        return Flux.concat(
            streamTuple.getT1(),
            content,
            streamTuple.getT2()
        );
    }

    static Flux<ByteBuffer> base64Encode(Flux<ByteBuffer> input) {

        final int buffer1Size = 1024 * 3;
        final int buffer2Size = 1024 * 4;
        byte[] buffer1 = new byte[buffer1Size];
        byte[] buffer2 = new byte[buffer2Size];
        AtomicInteger buffer1Free = new AtomicInteger(buffer1Size);
        AtomicInteger buffer2Written = new AtomicInteger(0);
        return input.handle((byteBuffer, sink) -> {
            if (byteBuffer.hasRemaining()) {
                int l = Math.min(byteBuffer.remaining(), buffer1Free.get());
                byteBuffer.get(buffer1, 0, l);
                buffer1Free.getAndAdd(-l);
                if (buffer1Free.get() == 0) {
                    // Convert ot Base64
                    int written = Base64.getEncoder().encode(buffer1, buffer2);
                    buffer1Free.set(buffer1Size); // reset input buffer
                    buffer2Written.getAndAdd(written);
                    // signal
                    sink.next(ByteBuffer.wrap(buffer2, 0, written));
                }
            } else {
                if (buffer2Written.get() > 0) {
                    int written = Base64.getEncoder().encode(buffer1, buffer2);
                    sink.next(ByteBuffer.wrap(buffer2, 0, written));
                    buffer1Free.set(buffer1Size);
                }
            }
        });
    }

    static Tuple2<Flux<ByteBuffer>, Flux<ByteBuffer>> splitToFlux(String input) {

        final Tuple2<String, String> stringTuple = split(input);
        return Tuples.of(
            Flux.just(ByteBuffer.wrap(stringTuple.getT1().getBytes(StandardCharsets.UTF_8))),
            Flux.just(ByteBuffer.wrap(stringTuple.getT2().getBytes(StandardCharsets.UTF_8))
            ));
    }

    static Tuple2<String, String> split(String input) {

        final int i = input.indexOf(CONTENT1);
        if (i == -1) {
            LOGGER.warn("No \"{}\" found in input \"{}\"", CONTENT1, input);
            return Tuples.of(input, "");
        }
        final String header = input.substring(0, i);
        final String footer = input.substring(i + CONTENT1.length());
        return Tuples.of(header, footer);
    }
}
