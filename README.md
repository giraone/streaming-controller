# Demo for streaming large data (upload, download) with Spring Boot WebFlux 

This includes a possibility to stream JSON or XML data containing Base64 encoded data.

## Build

```
mvn package
```

## Run

This will run the application with only 256MByte of memory.

```
./run.sh
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
  --silent \
  --output out.txt \
  http://localhost:8080/file/test.txt
```

This should return the file.

## Base64 included content within a JSON structure without having the content in memory

Download the uploaded file, where the content is Base64 encoded in a JSON structure.

```bash
curl --request GET \
  --silent \
  --output out.json \
  http://localhost:8080/base64/test.txt
```

This should return

```json
{"attribute1":"one", "attribute2":"MDEyMzQ1 ... ... NjcQ1Njc4OQ==", "attribute3":"three"}
```

where the Base64 value of *attribute1* part can be arbitrarily large.
