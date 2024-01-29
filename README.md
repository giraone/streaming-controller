# Demo for streaming large data (upload, download) with Spring Boot WebFlux

## Build

```
mvn package
```

## Usage

Upload a file

```bash
curl --request POST \
  --header "Content-Type: text/plain" \
  --data-binary @FILES/file-10k.bin \
  http://localhost:8080/file/test.txt
```

This should return

```json
{"success":true,"size":10240}
```

Download the uploaded file

```bash
curl --request GET \
  --output out.txt \
  http://localhost:8080/file/test.txt
```

This should return the file.

## Base64 included content within a JSON structure withour having the content in memory

Download the uploaded file, where the content in Base64 encoded in a JSON structure.

```bash
curl --request GET \
  --output out.txt \
  http://localhost:8080/base64/test.txt
```

This should return

```json
{"success":true, "size": 11111}
```