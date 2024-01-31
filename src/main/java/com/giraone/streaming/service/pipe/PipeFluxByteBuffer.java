package com.giraone.streaming.service.pipe;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.ByteBuffer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;

public final class PipeFluxByteBuffer {

    // Hide
    private PipeFluxByteBuffer() {
    }

    /**
     * Pipe an input Flux of ByteBuffer to an output Flux of ByteBuffer and apply an UnaryOperator on the ByteBuffer.
     * @param input the input Flux<ByteBuffer>
     * @param fct a function working on a ByteBuffer and resulting in a ByteBuffer
     * @return the output Flux of ByteBuffer
     */
    public static Flux<ByteBuffer> pipeByteBuffer(Flux<ByteBuffer> input, UnaryOperator<ByteBuffer> fct) {

        return input.map(fct::apply);
    }

    /**
     * Pipe an input Flux of ByteBuffer to an output Flux of ByteBuffer and apply a function on the byte array (chunks).
     * @param input the input Flux<ByteBuffer>
     * @param fct a function changing a byte block and returning a ByteArrayOutputPart
     * @return the output Flux of ByteBuffer
     */
    public static Flux<ByteBuffer> pipe(Flux<ByteBuffer> input, Function<byte[], ByteArrayOutputPart> fct) {

        return pipe(input, fct, null);
    }

    /**
     * Pipe an input Flux of ByteBuffer to an output Flux of ByteBuffer and apply a function on the byte array (chunks).
     * @param input the input Flux<ByteBuffer>
     * @param fct a function changing a byte block and returning a ByteArrayOutputPart
     * @param finalFct an optional function returning a ByteArrayOutput after the last input was processed
     * @return the output Flux of ByteBuffer
     */
    public static Flux<ByteBuffer> pipe(Flux<ByteBuffer> input,
                                        Function<byte[], ByteArrayOutputPart> fct,
                                        Supplier<ByteArrayOutputPart> finalFct) {

        if (finalFct != null) {
            return Flux.concat(pipeMain(input, fct), pipeLast(finalFct));
        } else {
            return pipeMain(input, fct);
        }
    }

    private static Flux<ByteBuffer> pipeMain(Flux<ByteBuffer> input, Function<byte[], ByteArrayOutputPart> fct) {

        return input.handle((byteBuffer, sink) -> {
            final byte[] inputBytes = new byte[byteBuffer.remaining()];
            byteBuffer.get(inputBytes);
            final ByteArrayOutputPart mappedOutputBytes = fct.apply(inputBytes);
            sink.next(ByteBuffer.wrap(mappedOutputBytes.array(), mappedOutputBytes.offset(), mappedOutputBytes.length()));
        });
    }

    private static Mono<ByteBuffer> pipeLast(Supplier<ByteArrayOutputPart> finalFct) {

        return Mono.fromCallable(() -> {
            final ByteArrayOutputPart mappedOutputBytes = finalFct.get();
            return ByteBuffer.wrap(mappedOutputBytes.array(), mappedOutputBytes.offset(), mappedOutputBytes.length());
        });
    }
}
