package com.giraone.streaming.controller;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.giraone.streaming.service.FluxUtil;
import com.giraone.streaming.service.base64.Base64Includer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.*;

@RestController
public class StreamingController {

    protected static final File FILE_BASE = new File("FILES");

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingController.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9-]+[.][a-z]{3,4}");

    @GetMapping("file/{filename}")
    ResponseEntity<Flux<ByteBuffer>> downloadFile(@PathVariable String filename) {

        if (isFileNameInvalid(filename)) {
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Invalid filename!".getBytes(StandardCharsets.UTF_8))));
        }
        final File file = new File(FILE_BASE, filename);
        final long contentLength = file.length();
        final AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(file.toPath(), READ);
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to read from \"{}\"!", file.getAbsolutePath(), e);
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Cannot open file!".getBytes(StandardCharsets.UTF_8))));
        }
        return streamToWebClient(FluxUtil.readFile(channel), MediaType.TEXT_PLAIN_VALUE, contentLength);
    }

    @GetMapping("base64/{filename}")
    ResponseEntity<Flux<ByteBuffer>> downloadFileBas64(@PathVariable String filename) {

        if (isFileNameInvalid(filename)) {
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Invalid filename!".getBytes(StandardCharsets.UTF_8))));
        }
        Map<String, Object> pojo = Map.of("attr1", "one", "content", Base64Includer.CONTENT1, "attr2", "two");
        String json;
        try {
            json = OBJECT_MAPPER.writeValueAsString(pojo);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Cannot create wrapper json from \"{}\"!", pojo, e);
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Cannot create wrapper json!".getBytes(StandardCharsets.UTF_8))));
        }
        final File file = new File(FILE_BASE, filename);
        final long contentLength = json.length() + file.length() - Base64Includer.CONTENT1.length();
        final AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(file.toPath(), READ);
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to read from \"{}\"!", file.getAbsolutePath(), e);
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Cannot open file!".getBytes(StandardCharsets.UTF_8))));
        }
        final Base64Includer base64Includer = new Base64Includer(json);
        return streamToWebClient(base64Includer.streamWithContent(FluxUtil.readFile(channel)), MediaType.TEXT_PLAIN_VALUE, contentLength);
    }

    @PostMapping("file/{filename}")
    Mono<ResponseEntity<Map<String, Object>>> uploadFile(@PathVariable String filename,
                                                         @RequestBody Flux<ByteBuffer> content,
                                                         @RequestHeader("Content-Length") Optional<String> contentLength) {

        if (isFileNameInvalid(filename)) {
            return Mono.just(ResponseEntity.badRequest().body(Map.of("success", false, "error", "Invalid filename!")));
        }
        final File file = new File(FILE_BASE, filename);
        final AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(file.toPath(), CREATE, WRITE);
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to write to \"{}\"!", file.getAbsolutePath(), e);
            return Mono.just(ResponseEntity.badRequest().body(Map.of("success", false,"error", "Cannot store file!")));
        }
        AtomicLong writtenBytes = new AtomicLong(0L);
        return FluxUtil.writeFile(content, channel)
            .doOnSuccess(voidIgnore -> {
                try {
                    channel.close();
                    writtenBytes.set(file.length());

                    LOGGER.warn("File \"{}\" with {} bytes written.", file.getAbsolutePath(), writtenBytes.get());
                } catch (IOException e) {
                    LOGGER.warn("Cannot close file \"{}\"!", file.getAbsolutePath(), e);
                }
            })
            .thenReturn(ResponseEntity.ok(Map.of(
                "success", true,
                "size", contentLength.orElse("-1").transform(Long::parseLong)
            )));
    }

    //------------------------------------------------------------------------------------------------------------------

    private boolean isFileNameInvalid(String filename) {
        return !FILE_NAME_PATTERN.matcher(filename).matches();
    }

    private ResponseEntity<Flux<ByteBuffer>> streamToWebClient(Flux<ByteBuffer> content, String mediaType, long contentLength) {

        return ResponseEntity
            .ok()
            .header("Content-Type", mediaType)
            .header("Content-Length", Long.toString(contentLength))
            .body(content);
    }
}
