package com.giraone.streaming.service.base64;

import com.giraone.streaming.service.pipe.ByteArrayOutputPart;
import com.giraone.streaming.service.pipe.PipeFluxByteBuffer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * A class to include base64 encoded content in JSON or XML structures.
 * A maximum of 9 tokens can be replaced by base64 encoded data streams.
 * Basically this is a kind of "templating", where variables are replaced by large data, that
 * is BASE64 encoded and fetched from a Flux<ByteBuffer>> stream.
 */
public class Base64Includer {

    private static final Logger LOGGER = LoggerFactory.getLogger(Base64Includer.class);

    public static final String CONTENT_TAG_1 = "<base64-1>";
    public static final String CONTENT_TAG_2 = "<base64-2>";
    public static final String CONTENT_TAG_3 = "<base64-3>";
    public static final String CONTENT_TAG_4 = "<base64-4>";
    public static final String CONTENT_TAG_5 = "<base64-5>";
    public static final String CONTENT_TAG_6 = "<base64-6>";
    public static final String CONTENT_TAG_7 = "<base64-7>";
    public static final String CONTENT_TAG_8 = "<base64-8>";
    public static final String CONTENT_TAG_9 = "<base64-9>";

    private final List<Flux<ByteBuffer>> streams;

    /**
     * Create new instance using the given template string (JSON, XML), that can contain
     * exactly one replacement token given by {@link ::CONTENT_TAG_1}
     * @param json a JSON string
     */
    public Base64Includer(String json) {
        this.streams = splitToFlux(json);
    }

    /**
     * Stream the stored JSON or XML structure together with the Base64 encoded content.
     * @param contents List of contents to be base64 encoded and included in the output.
     * @return an output Flux of ByteBuffers
     */
    public Flux<ByteBuffer> streamWithContent(List<Flux<ByteBuffer>> contents) {

        LOGGER.info("Stream {} contents with {} stream parts", contents.size(), streams.size());
        final List<Flux<ByteBuffer>> publishers = new ArrayList<>();
        int index = 0;
        for (; index < streams.size() - 1; index++) {
            publishers.add(streams.get(index));
            publishers.add(base64Encode(contents.get(index)));
        }
        publishers.add(streams.get(index));
        LOGGER.info("#Publishers = {}", publishers.size());
        return Flux.concat(publishers);
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

    static List<Flux<ByteBuffer>> splitToFlux(String input) {

        final List<String> strings = split(input);
        return strings.stream()
            .map(s -> Flux.just(ByteBuffer.wrap(s.getBytes(StandardCharsets.UTF_8))))
            .toList();
    }

    static List<String> split(String input) {

        String s = input;
        List<String> ret = new ArrayList<>();
        for (int index = 1; index <= 9; index++) {
            final String tag = "^(.*)(<base64-" + index + ">)(.*)$";
            final Pattern pattern = Pattern.compile(tag);
            final Matcher matcher = pattern.matcher(s);
            if (matcher.matches()) {
                final String before = s.substring(0, matcher.start(2));
                ret.add(before);
                s = s.substring(matcher.start(3));
            } else {
                ret.add(s);
                break;
            }
        }
        return ret;
    }
}
