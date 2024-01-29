package com.giraone.streaming.service.base64;

import com.giraone.streaming.service.FluxUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import reactor.core.publisher.Flux;
import reactor.util.function.Tuple2;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import static org.assertj.core.api.Assertions.assertThat;

class Base64IncluderTest {

    @ParameterizedTest
    @CsvSource(value = {
        "any,any,''",
        "header<base64-01>footer,header,footer"
    })
    void split(String jsonString, String expectedT1, String expectedT2) {

        // act
        Tuple2<String, String> t = Base64Includer.split(jsonString);
        // assert
        assertThat(t.getT1()).isEqualTo(expectedT1);
        assertThat(t.getT2()).isEqualTo(expectedT2);
    }

    @Test
    void base64Encode() {

        Flux<ByteBuffer> input = Flux.just(ByteBuffer.wrap("0123456789".getBytes(StandardCharsets.UTF_8)));
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        // act
        Flux<ByteBuffer> output = Base64Includer.base64Encode(input);
        FluxUtil.writeToOutputStream(output, byteArrayOutputStream).block();
        // assert
        assertThat(byteArrayOutputStream.toString(StandardCharsets.UTF_8)).isEqualTo("X");
    }
}