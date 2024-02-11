package com.giraone.streaming.controller;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.streaming.service.FluxUtil;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.MediaType;
import org.springframework.test.web.reactive.server.WebTestClient;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Flux;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest
@AutoConfigureWebTestClient
class StreamingControllerIT {

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingControllerIT.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final ParameterizedTypeReference<Map<String, Object>> MAP = new ParameterizedTypeReference<>() {
    };
    private static final TypeReference<Map<String, Object>> JSON_MAP = new TypeReference<>() {
    };

    @Autowired
    private WebTestClient webTestClient;

    @Test
    void downloadFile() throws IOException {

        final long expectedFileSize = 10240L;
        Flux<ByteBuffer> content = webTestClient.get()
            .uri("/file/file-10k.bin")
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_OCTET_STREAM)
            .expectHeader().contentLength(expectedFileSize)
            .returnResult(ByteBuffer.class)
            .getResponseBody();
        File target = File.createTempFile("test-", ".bin");
        LOGGER.info("Write to {}", target);
        target.deleteOnExit();
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(target.toPath(), CREATE, WRITE);
        FluxUtil.writeFile(content, channel).block();
        assertThat(target).exists().hasSize(expectedFileSize);
    }

    @Test
    void downloadJsonBase64() throws IOException {

        final int expectedFileSize = 10240 * 4 / 3;
        Flux<ByteBuffer> content = webTestClient.get()
            .uri("/base64/file-10k.bin")
            .exchange()
            .expectStatus().isOk()
            .expectHeader().contentType(MediaType.APPLICATION_JSON_VALUE)
            .returnResult(ByteBuffer.class)
            .getResponseBody();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        FluxUtil.writeToOutputStream(content,out).block();
        LOGGER.info("{} bytes received", out.size());
        assertThat(out.size()).isGreaterThan(expectedFileSize);
        Map<String,Object> json = OBJECT_MAPPER.readValue(out.toByteArray(), JSON_MAP);
        assertThat(json).containsKeys("attribute1", "attribute2", "attribute3");
        String base64 = (String) json.get("attribute2");
        assertThat(new String(Base64.getDecoder().decode(base64), StandardCharsets.UTF_8)).startsWith("0123456789");
    }

    @Test
    void uploadFileSmall() {
        String filename = "post-" + UUID.randomUUID() + ".txt";
        byte[] body = "0123456789".repeat(100).getBytes(StandardCharsets.UTF_8);
        int fileSize = body.length;
        webTestClient.post()
            .uri("/file/{filename}", filename)
            .contentType(MediaType.TEXT_PLAIN)
            .bodyValue(body)
            .exchange()
            .expectStatus().isOk()
            .expectBody(MAP)
            .value(value -> assertThat(value).containsExactlyInAnyOrderEntriesOf(Map.of(
                "success", true,
                "size", fileSize
            )));

        File target = new File(StreamingController.FILE_BASE, filename);
        assertThat(target).exists().hasSize(fileSize);
        assertThat(target.delete()).isTrue();
    }

    @Test
    void uploadFileLarge() {

        byte[] oneBuffer = "0123456789".repeat(100).getBytes(StandardCharsets.UTF_8);
        AtomicLong fileSize = new AtomicLong();
        Flux<ByteBuffer> publisher = Flux.range(1, 10).map(i -> {
            fileSize.getAndAdd(oneBuffer.length);
            return ByteBuffer.wrap(oneBuffer);
        });
        var bodyInserter = BodyInserters.fromPublisher(publisher, ByteBuffer.class);
        String filename = "post-" + UUID.randomUUID() + ".txt";
        webTestClient.post()
            .uri("/file/{filename}", filename)
            .contentType(MediaType.TEXT_PLAIN)
            .body(bodyInserter)
            .exchange()
            .expectStatus().isOk()
            .expectBody(MAP)
            .value(value -> assertThat(value).containsExactlyInAnyOrderEntriesOf(Map.of(
                "success", true,
                "size", -1
            )));

        File target = new File(StreamingController.FILE_BASE, filename);
        assertThat(target).exists().hasSize(fileSize.get());
        assertThat(target.delete()).isTrue();
    }
}