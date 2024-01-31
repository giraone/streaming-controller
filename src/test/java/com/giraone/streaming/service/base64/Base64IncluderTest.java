package com.giraone.streaming.service.base64;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.streaming.controller.StreamingController;
import com.giraone.streaming.service.FluxUtil;
import com.giraone.streaming.service.pipe.ByteArrayOutputPart;
import com.giraone.streaming.util.ObjectMapperBuilder;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;

import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;

class Base64IncluderTest {

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperBuilder.build(
        false, false, true, false);

    @ParameterizedTest
    @CsvSource({
        "any,any,''",
        "header<base64-1>footer,header,footer"
    })
    void split(String jsonString, String expectedT1, String expectedT2) {

        // act
        Tuple2<String, String> t = Base64Includer.split(jsonString);
        // assert
        assertThat(t.getT1()).isEqualTo(expectedT1);
        assertThat(t.getT2()).isEqualTo(expectedT2);
    }

    @ParameterizedTest
    @CsvSource({
        "0123456789012345678901234567890,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
        "01234567890123456789012345678901,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=",
        "012345678901234567890123456789012,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEy",
    })
    void encode(String inputString, String expected) {

        // arrange
        byte[] input = inputString.getBytes(StandardCharsets.UTF_8);
        // act
        ByteArrayOutputPart output = Base64Includer.encode(input);
        // assert - compare to expected
        String outputString = output.toString();
        assertThat(outputString).isEqualTo(expected);
        // assert - compare to expected decoding again
        String decodedAgain = new String(Base64.getDecoder().decode(outputString.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        assertThat(decodedAgain).isEqualTo(inputString);
    }

    @ParameterizedTest
    @CsvSource({
        "'',0,'',''",
        "x,1,'',x",
        "xy,2,'',xy",
        "xy,2,01,xy01",
        "'',0,01,01",
    })
    void buildArray(String s1, int l1, String s2, String expected) {

        // act
        byte[] actual = Base64Includer.buildArray(s1.getBytes(StandardCharsets.UTF_8), l1, s2.getBytes(StandardCharsets.UTF_8));
        // assert
        assertThat(new String(actual, StandardCharsets.UTF_8)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "xyz,xyz",
        "xyz,xy",
        "xyz,x",
        "xyz,''",
        "xy,xyz",
        "x,xyz",
        "'',xyz",
    })
    void base64Encode_small(String part1, String part2) {

        String expectedDecoded = part1 + part2;
        String expectedEncoded = new String(Base64.getEncoder().encode(expectedDecoded.getBytes(StandardCharsets.UTF_8)), StandardCharsets.UTF_8);
        Flux<ByteBuffer> input = Flux.concat(
            Flux.just(ByteBuffer.wrap(part1.getBytes(StandardCharsets.UTF_8))),
            Flux.just(ByteBuffer.wrap(part2.getBytes(StandardCharsets.UTF_8)))
        );
        // act
        Flux<ByteBuffer> output = Base64Includer.base64Encode(input);
        // assert
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();
        assertThat(byteArrayOutputStream).hasToString(expectedEncoded);
    }

    @ParameterizedTest
    @CsvSource({
        "0,3072", // multiple of 3 - the easy part
        "1,3072", // multiple of 3 - the easy part
        "2,3072", // multiple of 3 - the easy part
        "0,4096", // not a multiple of 3 - the hard part
        "1,4096", // not a multiple of 3 - the hard part
        "2,4096", // not a multiple of 3 - the hard part
    })
    void base64Encode_large(int delta, Integer chunkSize) throws IOException {

        // arrange
        File file = new File(StreamingController.FILE_BASE, "file-10k.bin");
        long fileSize = file.length();
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(file.toPath(), READ);
        String addon = "x".repeat(delta);
        Flux<ByteBuffer> deltaFlux = Flux.just(ByteBuffer.wrap(addon.getBytes(StandardCharsets.UTF_8)));
        Flux<ByteBuffer> input = Flux.concat(FluxUtil.readFile(channel, chunkSize), deltaFlux);
        int expectedOutputSize = (int) ((fileSize + 1 + delta) * 4 / 3);
        // act
        Flux<ByteBuffer> output = Base64Includer.base64Encode(input);
        // assert
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();
        byte[] outputAsByteArray = byteArrayOutputStream.toByteArray();
        // assert
        String decodedAgain = new String(Base64.getDecoder().decode(outputAsByteArray));
        assertThat(decodedAgain)
            .startsWith("0123456789")
            .endsWith("0123456789" + addon);
        // just a plausibility check - the exact number depends on the trailing = characters
        assertThat(outputAsByteArray.length).isCloseTo(expectedOutputSize, Offset.offset(2));
    }

    @ParameterizedTest
    @CsvSource({
        "0123456789012345678901234567890,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==",
        "01234567890123456789012345678901,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=",
        "012345678901234567890123456789012,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEy",
    })
    void streamWithContent(String fluxInputString, String fluxOutputBase64) throws JsonProcessingException {

        // arrange
        Object pojo = Map.of("attr1", "value1", "attr2", Base64Includer.CONTENT_TAG_1, "attr3", "value3");
        String json = OBJECT_MAPPER.writeValueAsString(pojo);
        Flux<ByteBuffer> input = Flux.just(ByteBuffer.wrap(fluxInputString.getBytes(StandardCharsets.UTF_8)));

        // act
        Base64Includer base64Includer = new Base64Includer(json);
        Flux<ByteBuffer> output = base64Includer.streamWithContent(input);
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();

        // assert
        String expected = "{\"attr1\":\"value1\",\"attr2\":\"" + fluxOutputBase64 + "\",\"attr3\":\"value3\"}";
        assertThat(byteArrayOutputStream.toString(StandardCharsets.UTF_8)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "0,0",
        "1,4",
        "2,4",
        "3,4",
        "4,8",
    })
    void calculateBase64Size(int input, int expected) {

        assertThat(Base64Includer.calculateBase64Size(input)).isEqualTo(expected);
    }
}