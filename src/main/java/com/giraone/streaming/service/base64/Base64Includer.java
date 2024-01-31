package com.giraone.streaming.service.base64;

import com.giraone.streaming.service.pipe.ByteArrayOutputPart;
import com.giraone.streaming.service.pipe.PipeFluxByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;
import reactor.util.function.Tuples;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * A class to include base64 encoded content in JSON or XML structures.
 * Basically this is a kind of "templating", where variables are replaced by large data, that
 * is BASE64 encoded and fetched from a Flux<ByteBuffer>> stream.
 */
public class Base64Includer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Base64Includer.class);
    public static final String CONTENT_TAG_1 = "<base64-1>";

    private final Tuple2<Flux<ByteBuffer>, Flux<ByteBuffer>> streamTuple;

    /**
     * Create new instance using the given template string (JSON, XML), that can contain
     * exactly one replacement token given by {@link ::CONTENT_TAG_1}
     * @param json a JSON string
     */
    public Base64Includer(String json) {
        this.streamTuple = splitToFlux(json);
    }

    /**
     * Stream the stored JSON or XML structure together with the Base64 encoded content.
     * @param content Content to be base64 encoded and included in the output.
     * @return an output Flux of ByteBuffers
     */
    public Flux<ByteBuffer> streamWithContent(Flux<ByteBuffer> content) {
        return Flux.concat(
            streamTuple.getT1(),
            base64Encode(content),
            streamTuple.getT2()
        );
    }

    /**
     * Base64 encode a given Flux of ByteBuffers
     * @param inputFlux the input Flux of ByteBuffers
     * @return an output Flux of ByteBuffers
     */
    public static Flux<ByteBuffer> base64Encode(Flux<ByteBuffer> inputFlux) {

        // Amount of remaining part (0-2)
        final AtomicInteger lastOverhead = new AtomicInteger(0);
        // The last 0-2 bytes of the remaining part
        final byte[] lastOverheadBuffer = new byte[2];
        final Function<byte[], ByteArrayOutputPart> fct = bytes -> {
            final byte[] combinedInput = Base64Includer.buildArray(lastOverheadBuffer, lastOverhead.get(), bytes);
            final int overheadSize = combinedInput.length % 3;
            final int rawTargetByteSizeWithoutPadding = combinedInput.length * 4 / 3;
            final byte[] result = new byte[rawTargetByteSizeWithoutPadding + 4];
            Base64.getEncoder().encode(combinedInput, result);
            lastOverhead.set(overheadSize);
            System.arraycopy(combinedInput, combinedInput.length - overheadSize, lastOverheadBuffer, 0, overheadSize);
            return new ByteArrayOutputPart(result, 0, rawTargetByteSizeWithoutPadding - (rawTargetByteSizeWithoutPadding % 4));
        };
        final Supplier<ByteArrayOutputPart> finalFct = () -> {
            final byte[] input = new String(lastOverheadBuffer, 0, lastOverhead.get(), StandardCharsets.UTF_8).getBytes(StandardCharsets.UTF_8);
            final byte[] result = new byte[4];
            final int encoded = Base64.getEncoder().encode(input, result);
            return new ByteArrayOutputPart(result, 0, encoded);
        };
        return PipeFluxByteBuffer.pipe(inputFlux, fct, finalFct);
    }

    public static int calculateBase64Size(int inputSize) {
        int ret = 4 * inputSize / 3;
        if (inputSize % 3 != 0) {
            ret += 4 - (inputSize % 3);
        }
        return ret;
    }

    //------------------------------------------------------------------------------------------------------------------

    static byte[] buildArray(byte[] buffer1, int length1, byte[] buffer2) {
        if (length1 == 0) {
            return buffer2;
        }
        final byte[] newBuffer = new byte[length1 + buffer2.length];
        System.arraycopy(buffer1, 0, newBuffer, 0, length1);
        System.arraycopy(buffer2, 0, newBuffer, length1, buffer2.length);
        return newBuffer;
    }

    static ByteArrayOutputPart encode(byte[] input) {
        final byte[] output = new byte[input.length * 4 / 3 + 3];
        final int encoded = Base64.getEncoder().encode(input, output);
        return new ByteArrayOutputPart(output, 0, encoded);
    }

    static Tuple2<Flux<ByteBuffer>, Flux<ByteBuffer>> splitToFlux(String input) {

        final Tuple2<String, String> stringTuple = split(input);
        return Tuples.of(
            Flux.just(ByteBuffer.wrap(stringTuple.getT1().getBytes(StandardCharsets.UTF_8))),
            Flux.just(ByteBuffer.wrap(stringTuple.getT2().getBytes(StandardCharsets.UTF_8)))
        );
    }

    static Tuple2<String, String> split(String input) {

        final int i = input.indexOf(CONTENT_TAG_1);
        if (i == -1) {
            LOGGER.warn("No \"{}\" found in input \"{}\"", CONTENT_TAG_1, input);
            return Tuples.of(input, "");
        }
        final String header = input.substring(0, i);
        final String footer = input.substring(i + CONTENT_TAG_1.length());
        return Tuples.of(header, footer);
    }
}
