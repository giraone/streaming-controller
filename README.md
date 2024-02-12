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
  http://localhost:8080/base64-1/test1.txt
```

This should return

```json
{"attribute1":"one","attribute2":"SGVsbG8gRXVyb3BlIQ==","attribute3":"three"}
```

```bash 
curl --request GET \
  --silent \
  --output out.json \
  http://localhost:8080/base64-2/test1.txt/test2.txt  
```

This should return

```json
{"attribute1":"one","attribute2":"SGVsbG8gRXVyb3BlIQ==","attribute3":"three","attribute4":"SGVsbG8gV29ybGQh","attribute5":"five"}
```

where the Base64 value of *attribute2* and *attribute4* can be arbitrarily large, when other files are used.
