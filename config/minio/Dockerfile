FROM golang:1.24.13-bookworm AS build
RUN CGO_ENABLED=0 go install github.com/minio/minio@RELEASE.2025-10-15T17-29-55Z
FROM scratch
COPY --from=build /go/bin/minio /usr/local/bin/minio
EXPOSE 9000 9001
ENTRYPOINT ["minio"]
CMD ["server", "/data", "--console-address", ":9001"]
