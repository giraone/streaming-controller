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

    public static final File FILE_BASE = new File("FILES");

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingController.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9-]+[.][a-z]{3,4}");
    private static final String ATTR_SUCCESS = "success";
    private static final String ATTR_SIZE = "size";
    private static final String ATTR_ERROR = "error";

    @GetMapping("file/{filename}")
    ResponseEntity<Flux<ByteBuffer>> downloadFile(@PathVariable String filename) {

        if (isFileNameInvalid(filename)) {
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Invalid download filename!".getBytes(StandardCharsets.UTF_8))));
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
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Invalid inclusion filename!".getBytes(StandardCharsets.UTF_8))));
        }

        // An example for a JSON Java Pojo with one replacement token
        Map<String, Object> pojo = Map.of("attribute1", "one", "attribute2", Base64Includer.CONTENT_TAG_1, "attribute3", "three");
        String json;
        try {
            json = OBJECT_MAPPER.writeValueAsString(pojo);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Cannot create wrapper json from \"{}\"!", pojo, e);
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Cannot create wrapper json!".getBytes(StandardCharsets.UTF_8))));
        }

        // A file, that is read an included in the output as a replacement for the token
        final File file = new File(FILE_BASE, filename);
        // If we want to support a content length in the HTTP response header, we can use this utility
        final long contentLength = json.length() + Base64Includer.calculateBase64Size((int) file.length()) - Base64Includer.CONTENT_TAG_1.length();
        final AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(file.toPath(), READ);
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to read from \"{}\"!", file.getAbsolutePath(), e);
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Cannot open file!".getBytes(StandardCharsets.UTF_8))));
        }
        final Base64Includer base64Includer = new Base64Includer(json);
        // Now we have a Flux<ByteBuffer>, that is the input for the "base64Includer"
        final Flux<ByteBuffer> inputByteBufferFlux = FluxUtil.readFile(channel);
        return streamToWebClient(base64Includer.streamWithContent(inputByteBufferFlux), MediaType.APPLICATION_JSON_VALUE, contentLength);
    }

    @PostMapping("file/{filename}")
    Mono<ResponseEntity<Map<String, Object>>> uploadFile(@PathVariable String filename,
                                                         @RequestBody Flux<ByteBuffer> content,
                                                         @RequestHeader("Content-Length") Optional<String> contentLength) {

        if (isFileNameInvalid(filename)) {
            return Mono.just(ResponseEntity.badRequest().body(Map.of(ATTR_SUCCESS, false, ATTR_ERROR, "Invalid target filename!")));
        }
        final File file = new File(FILE_BASE, filename);
        final AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(file.toPath(), CREATE, WRITE);
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to write to \"{}\"!", file.getAbsolutePath(), e);
            return Mono.just(ResponseEntity.badRequest().body(Map.of(ATTR_SUCCESS, false, ATTR_ERROR, "Cannot store file!")));
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
                ATTR_SUCCESS, true,
                ATTR_SIZE, contentLength.orElse("-1").transform(Long::parseLong)
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

    private ResponseEntity<Flux<ByteBuffer>> streamToWebClient(Flux<ByteBuffer> content, String mediaType) {

        return ResponseEntity
            .ok()
            .header("Content-Type", mediaType)
            .body(content);
    }
}
