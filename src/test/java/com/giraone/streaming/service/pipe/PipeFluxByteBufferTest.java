package com.giraone.streaming.service.pipe;

import com.giraone.streaming.controller.StreamingController;
import com.giraone.streaming.service.FluxUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.util.function.Function;
import java.util.function.Supplier;

import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;

class PipeFluxByteBufferTest {

    private static final String SMALL = "abcdefghijklmnopqrstuvwxyz01234567890123456789abcdefghijklmnopqrstuvwxyz";
    private static final String FOOTER = "FOO";

    private static final Function<byte[], ByteArrayOutputPart> FCT_SANITIZE1 = bytes -> {
        final String s = new String(bytes, StandardCharsets.UTF_8);
        final byte[] result = s.replaceAll("[^a-zA-Z]", "").getBytes(StandardCharsets.UTF_8);
        return new ByteArrayOutputPart(result, 0, result.length);
    };
    private static final Function<byte[], ByteArrayOutputPart> FCT_SANITIZE2 = bytes -> {
        final String s = new String(bytes, StandardCharsets.UTF_8);
        final byte[] result = s.replaceAll("[02468]", "").getBytes(StandardCharsets.UTF_8);
        return new ByteArrayOutputPart(result, 0, result.length);
    };
    private static final Function<byte[], ByteArrayOutputPart> FCT_COUNTER = bytes -> {
        final byte[] result = String.format("%d-", bytes.length).getBytes();
        return new ByteArrayOutputPart(result, 0, result.length);
    };
    private static final Supplier<ByteArrayOutputPart> FINAL_FCT = () -> {
        final byte[] footer = FOOTER.getBytes(StandardCharsets.UTF_8);
        return new ByteArrayOutputPart(footer, 0, footer.length);
    };

    @Test
    void pipeNoOpSmall_noChunks() {

        // arrange
        Function<byte[], ByteArrayOutputPart> fct = bytes -> new ByteArrayOutputPart(bytes, 0, bytes.length);
        // act
        String output = pipeToString(buildSmallFluxByteBuffer(), fct, null);
        // assert
        assertThat(output).isEqualTo(SMALL);
    }

    @Test
    void pipeSanitizeSmall_noChunks() {

        // act
        String output = pipeToString(buildSmallFluxByteBuffer(), FCT_SANITIZE1, null);
        // assert
        assertThat(output).isEqualTo("abcdefghijklmnopqrstuvwxyzabcdefghijklmnopqrstuvwxyz");
    }

    @ParameterizedTest
    @CsvSource({
        "1024,",
        "4096,"
    })
    void pipeSanitizeLarge(Integer chunkSize) throws IOException {

        // arrange
        File file = new File(StreamingController.FILE_BASE, "file-10k.bin");
        long fileSize = file.length();
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(), READ);
        Flux<ByteBuffer> input = FluxUtil.readFile(channel, chunkSize);
        int expectedOutputSize = (int) fileSize / 2;
        // act
        String output = pipeToString(input, FCT_SANITIZE2, null);
        // assert
        assertThat(output).hasSize(expectedOutputSize);
    }

    @ParameterizedTest
    @CsvSource({
        "1023,1023-1023-1023-1023-1023-1023-1023-1023-1023-1023-10-FOO",
        "1024,1024-1024-1024-1024-1024-1024-1024-1024-1024-1024-FOO",
        "1025,1025-1025-1025-1025-1025-1025-1025-1025-1025-1015-FOO",
        "10240,10240-FOO"
    })
    void pipeWithFinalFct(Integer chunkSize, String expected) throws IOException {

        // arrange
        File file = new File(StreamingController.FILE_BASE, "file-10k.bin");
        long fileSize = file.length();
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(), READ);
        Flux<ByteBuffer> input = FluxUtil.readFile(channel, chunkSize);
        int expectedOutputSize = (int) fileSize / 2 + FOOTER.length();
        // act
        Flux<ByteBuffer> output = PipeFluxByteBuffer.pipe(input, FCT_COUNTER, FINAL_FCT);
        // assert
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();
        String outputAsString = byteArrayOutputStream.toString(StandardCharsets.UTF_8);
        assertThat(outputAsString).isEqualTo(expected);
    }

    //------------------------------------------------------------------------------------------------------------------

    private Flux<ByteBuffer> buildSmallFluxByteBuffer() {
        return Flux.just(ByteBuffer.wrap(SMALL.getBytes(StandardCharsets.UTF_8)));
    }

    String pipeToString(Flux<ByteBuffer> input,
                        Function<byte[], ByteArrayOutputPart> fct,
                        Supplier<ByteArrayOutputPart> finalFct) {

        Flux<ByteBuffer> output = PipeFluxByteBuffer.pipe(input, fct, finalFct);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();
        return byteArrayOutputStream.toString(StandardCharsets.UTF_8);
    }
}