package com.giraone.streaming.controller;

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
import org.springframework.web.reactive.function.BodyInserter;
import org.springframework.web.reactive.function.BodyInserters;
import reactor.core.publisher.Flux;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
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

    private static final ParameterizedTypeReference<Map<String, Object>> MAP = new ParameterizedTypeReference<>() {
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
            .expectHeader().contentType(MediaType.TEXT_PLAIN)
            .expectHeader().contentLength(expectedFileSize)
            .returnResult(ByteBuffer.class)
            .getResponseBody();
        File target = File.createTempFile("test-", ".txt");
        LOGGER.info("Write to {}", target);
        target.deleteOnExit();
        AsynchronousFileChannel channel = AsynchronousFileChannel.open(target.toPath(), CREATE, WRITE);
        FluxUtil.writeFile(content, channel).block();
        assertThat(target.exists()).isTrue();
        assertThat(target.length()).isEqualTo(expectedFileSize);
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
        assertThat(target.exists()).isTrue();
        assertThat(target.length()).isEqualTo(fileSize);
        assertThat(target.delete()).isTrue();
    }

    @Test
    void uploadFileLarge() {

        byte[] oneBuffer = "0123456789".repeat(100).getBytes(StandardCharsets.UTF_8);
        AtomicLong fileSize = new AtomicLong();
        Flux<ByteBuffer> publisher = Flux.range(1,10).map(i -> { fileSize.getAndAdd(oneBuffer.length); return ByteBuffer.wrap(oneBuffer); });
        BodyInserter bodyInserter = BodyInserters.fromPublisher(publisher, ByteBuffer.class);
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
        assertThat(target.exists()).isTrue();
        assertThat(target.length()).isEqualTo(fileSize.get());
        assertThat(target.delete()).isTrue();
    }
}