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
import java.nio.file.NoSuchFileException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;

import static java.nio.file.StandardOpenOption.*;

@RestController
public class StreamingController {

    public static final File FILE_BASE = new File("FILES");
    public static final String X_HEADER_ERROR = "X-Files-Error";

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingController.class);
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Pattern FILE_NAME_PATTERN = Pattern.compile("[a-zA-Z0-9-]+[.][a-z]{3,4}");
    private static final String ATTR_SUCCESS = "success";
    private static final String ATTR_SIZE = "size";
    private static final String ATTR_ERROR = "error";

    @SuppressWarnings("unused")
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

    @SuppressWarnings("unused")
    @GetMapping("file/{filename}")
    ResponseEntity<Flux<ByteBuffer>> downloadFile(@PathVariable String filename) {

        if (isFileNameInvalid(filename)) {
            return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Invalid download filename!").build();
        }
        final File file = new File(FILE_BASE, filename);
        final MediaType mediaType = mediaTypeFromFileName(filename);
        final long contentLength = file.length();
        final AsynchronousFileChannel channel;
        try {
            channel = AsynchronousFileChannel.open(file.toPath(), READ);
        } catch (NoSuchFileException nsfe) {
            LOGGER.warn("File \"{}\" does not exist! {}", file.getAbsolutePath(), nsfe.getMessage());
            return ResponseEntity.notFound().header(X_HEADER_ERROR, "File does not exist!").build();
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to read from \"{}\"! {}", file.getAbsolutePath(), e.getMessage());
            return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Cannot read file!").build();
        }
        return streamToWebClient(FluxUtil.readFile(channel), mediaType.toString(), contentLength);
    }

    @SuppressWarnings("unused")
    @GetMapping("base64-1/{filename}")
    ResponseEntity<Flux<ByteBuffer>> downloadFile1Base64(@PathVariable String filename) {

        if (isFileNameInvalid(filename)) {
            return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Invalid inclusion filename!").build();
        }

        // An example for a JSON Java Pojo with one replacement token
        Map<String, Object> pojo = Map.of(
            "attribute1", "one",
            "attribute2", Base64Includer.CONTENT_TAG_1,
            "attribute3", "three"
        );
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
        } catch (NoSuchFileException nsfe) {
            LOGGER.warn("File \"{}\" does not exist! {}", file.getAbsolutePath(), nsfe.getMessage());
            return ResponseEntity.notFound().header(X_HEADER_ERROR, "File does not exist!").build();
        } catch (IOException e) {
            LOGGER.warn("Cannot open file to read from \"{}\"! {}", file.getAbsolutePath(), e.getMessage());
            return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Cannot read file!").build();
        }
        final Base64Includer base64Includer = new Base64Includer(json);
        // Now we have a Flux<ByteBuffer>, that is the input for the "base64Includer"
        final Flux<ByteBuffer> inputByteBufferFlux = FluxUtil.readFile(channel);
        return streamToWebClient(base64Includer.streamWithContent(List.of(inputByteBufferFlux)), MediaType.APPLICATION_JSON_VALUE, contentLength);
    }

    @SuppressWarnings("unused")
    @GetMapping("base64-2/{filename1}/{filename2}")
    ResponseEntity<Flux<ByteBuffer>> downloadFile2Bas64(@PathVariable String filename1, @PathVariable String filename2) {

        if (isFileNameInvalid(filename1)) {
            return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Invalid inclusion filename1!").build();
        }
        if (isFileNameInvalid(filename2)) {
            return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Invalid inclusion filename2!").build();
        }

        // An example for a JSON Java Pojo with one replacement token
        Map<String, Object> pojo = Map.of(
            "attribute1", "one",
            "attribute2", Base64Includer.CONTENT_TAG_1,
            "attribute3", "three",
            "attribute4", Base64Includer.CONTENT_TAG_2,
            "attribute5", "five"
        );
        String json;
        try {
            json = OBJECT_MAPPER.writeValueAsString(pojo);
        } catch (JsonProcessingException e) {
            LOGGER.warn("Cannot create wrapper json from \"{}\"!", pojo, e);
            return ResponseEntity.badRequest().body(Flux.just(ByteBuffer.wrap("Cannot create wrapper json!".getBytes(StandardCharsets.UTF_8))));
        }

        final List<String> filenames = List.of(filename1, filename2);
        final List<FileWithChannel> files = new ArrayList<>();
        final List<Flux<ByteBuffer>> streams = new ArrayList<>();
        long contentLength = json.length();
        for (String filename : filenames) {
            final File file = new File(FILE_BASE, filename);
            final long base64Size = Base64Includer.calculateBase64Size((int) file.length());
            contentLength += base64Size - Base64Includer.CONTENT_TAG_1.length();
            final AsynchronousFileChannel channel;
            try {
                channel = AsynchronousFileChannel.open(file.toPath(), READ);
            } catch (NoSuchFileException nsfe) {
                LOGGER.warn("File \"{}\" does not exist! {}", file.getAbsolutePath(), nsfe.getMessage());
                return ResponseEntity.notFound().header(X_HEADER_ERROR, "File does not exist!").build();
            } catch (IOException e) {
                LOGGER.warn("Cannot open file to read from \"{}\"! {}", file.getAbsolutePath(), e.getMessage());
                return ResponseEntity.badRequest().header(X_HEADER_ERROR, "Cannot read file!").build();
            }
            files.add(new FileWithChannel(file, channel));
            final Flux<ByteBuffer> inputByteBufferFlux = FluxUtil.readFile(channel);
            streams.add(inputByteBufferFlux);
            LOGGER.info("File \"{}\" opened", file);
        }

        LOGGER.info("Calculated content-length={}", contentLength);
        final Base64Includer base64Includer = new Base64Includer(json);
        final Flux<ByteBuffer> output = base64Includer.streamWithContent(streams)
            .doAfterTerminate(() -> closeChannels(files));
        // Now we have a Flux<ByteBuffer>, that is the input for the "base64Includer"
        return streamToWebClient(output, MediaType.APPLICATION_JSON_VALUE, contentLength);
    }

    //------------------------------------------------------------------------------------------------------------------

    private static boolean isFileNameInvalid(String filename) {
        return !FILE_NAME_PATTERN.matcher(filename).matches();
    }

    private static MediaType mediaTypeFromFileName(String filename) {

        if (filename.endsWith(".txt")) {
            return MediaType.TEXT_PLAIN;
        } else {
            return MediaType.APPLICATION_OCTET_STREAM;
        }
    }

    private static ResponseEntity<Flux<ByteBuffer>> streamToWebClient(Flux<ByteBuffer> content, String mediaType, long contentLength) {

        return ResponseEntity
            .ok()
            .header("Content-Type", mediaType)
            .header("Content-Length", Long.toString(contentLength))
            .body(content);
    }

    private static void closeChannels(List<FileWithChannel> fileWithChannels) {

        fileWithChannels.stream().forEach(fileWithChannel -> {
            try {
                fileWithChannel.channel.close();
                LOGGER.warn("Channel {} closed", fileWithChannel.file);
            } catch (IOException e) {
                LOGGER.warn("Cannot close {}", fileWithChannel.file, e);
            }
        });
    }

    record FileWithChannel(File file, AsynchronousFileChannel channel) {

    }
}
