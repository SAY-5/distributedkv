# syntax=docker/dockerfile:1.7
FROM golang:1.23-alpine AS build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /out/kvd        ./cmd/kvd      && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /out/kvctl      ./cmd/kvctl    && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /out/loadgen    ./cmd/loadgen  && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /out/faultctl   ./cmd/faultctl && \
    CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" \
      -o /out/singlenode ./cmd/singlenode

FROM alpine:3.20
RUN adduser -D -u 10001 dkv && apk add --no-cache ca-certificates
USER dkv
WORKDIR /srv
COPY --from=build /out/kvd        /usr/local/bin/kvd
COPY --from=build /out/kvctl      /usr/local/bin/kvctl
COPY --from=build /out/loadgen    /usr/local/bin/loadgen
COPY --from=build /out/faultctl   /usr/local/bin/faultctl
COPY --from=build /out/singlenode /usr/local/bin/singlenode
COPY --chown=dkv:dkv web /srv/web
EXPOSE 7001 8001
HEALTHCHECK --interval=10s --timeout=2s --retries=3 \
  CMD wget -qO- http://127.0.0.1:8001/v1/healthz || exit 1
ENTRYPOINT ["/usr/local/bin/kvd"]
CMD ["-id", "n1", "-raft", ":7001", "-http", ":8001", "-data", "/srv/data", "-web", "/srv/web"]
