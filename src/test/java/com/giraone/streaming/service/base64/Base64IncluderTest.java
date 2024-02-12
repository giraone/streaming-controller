package com.giraone.streaming.service.base64;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.streaming.controller.StreamingController;
import com.giraone.streaming.service.FluxUtil;
import com.giraone.streaming.service.pipe.ByteArrayOutputPart;
import com.giraone.streaming.util.ObjectMapperBuilder;
import org.assertj.core.data.Offset;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;
import java.util.Map;

import static java.nio.file.StandardOpenOption.READ;
import static org.assertj.core.api.Assertions.assertThat;

class Base64IncluderTest {

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperBuilder.build(
        false, false, true, false);

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
        "any,any,,,",
        "header<base64-1>footer,header,footer,,",
        "header<base64-1>intermediate1<base64-2>footer,header,intermediate1,footer,",
        "header<base64-1>intermediate1<base64-2>intermediate2<base64-3>footer,header,intermediate1,intermediate2,footer"
    })
    void split(String jsonString, String expected0, String expected1, String expected2, String expected3) {

        // act
        List<String> t = Base64Includer.split(jsonString);
        // assert
        assertThat(t.get(0)).isEqualTo(expected0);
        if (expected1 != null) {
            assertThat(t.get(1)).isEqualTo(expected1);
        }
        if (expected2 != null) {
            assertThat(t.get(2)).isEqualTo(expected2);
        }
        if (expected3 != null) {
            assertThat(t.get(3)).isEqualTo(expected3);
        }
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
    void streamWithContent2(String fluxInputString, String fluxOutputBase64) throws JsonProcessingException {

        // arrange
        Object pojo = Map.of(
            "attribute1", "value1",
            "attribute2", Base64Includer.CONTENT_TAG_1,
            "attribute3", "value3"
        );
        String json = OBJECT_MAPPER.writeValueAsString(pojo);
        Flux<ByteBuffer> input = Flux.just(ByteBuffer.wrap(fluxInputString.getBytes(StandardCharsets.UTF_8)));

        // act
        Base64Includer base64Includer = new Base64Includer(json);
        Flux<ByteBuffer> output = base64Includer.streamWithContent(List.of(input));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();

        // assert
        String expected = "{\"attribute1\":\"value1\",\"attribute2\":\"" + fluxOutputBase64 + "\",\"attribute3\":\"value3\"}";
        assertThat(byteArrayOutputStream.toString(StandardCharsets.UTF_8)).isEqualTo(expected);
    }

    @ParameterizedTest
    @CsvSource({
        "0123456789012345678901234567890,01234567890123456789012345678901,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MA==,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=",
        "01234567890123456789012345678901,012345678901234567890123456789012,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDE=,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEy",
        "012345678901234567890123456789012,0123456789012345678901234567890123,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEy,MDEyMzQ1Njc4OTAxMjM0NTY3ODkwMTIzNDU2Nzg5MDEyMw==",
    })
    void streamWithContent3(String fluxInputString1, String fluxInputString2, String fluxOutput1Base64, String fluxOutput2Base64) throws JsonProcessingException {

        // arrange
        Object pojo = Map.of(
            "attribute1", "value1",
            "attribute2", Base64Includer.CONTENT_TAG_1,
            "attribute3", "value3",
            "attribute4", Base64Includer.CONTENT_TAG_2,
            "attribute5", "value5"
        );
        String json = OBJECT_MAPPER.writeValueAsString(pojo);
        Flux<ByteBuffer> input1 = Flux.just(ByteBuffer.wrap(fluxInputString1.getBytes(StandardCharsets.UTF_8)));
        Flux<ByteBuffer> input2 = Flux.just(ByteBuffer.wrap(fluxInputString2.getBytes(StandardCharsets.UTF_8)));

        // act
        Base64Includer base64Includer = new Base64Includer(json);
        Flux<ByteBuffer> output = base64Includer.streamWithContent(List.of(input1, input2));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();

        // assert
        String expected = "{\"attribute1\":\"value1\",\"attribute2\":\""
            + fluxOutput1Base64 + "\",\"attribute3\":\"value3\",\"attribute4\":\""
            + fluxOutput2Base64 + "\",\"attribute5\":\"value5\"}";
        assertThat(byteArrayOutputStream.toString(StandardCharsets.UTF_8)).isEqualTo(expected);
    }
}
