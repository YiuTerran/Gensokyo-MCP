FROM --platform=$BUILDPLATFORM golang:1.24-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
RUN apk add --no-cache ca-certificates git
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o /out/gensokyo-mcp .

FROM alpine:3.22
RUN apk add --no-cache ca-certificates tzdata && adduser -D -H -u 10001 app && mkdir -p /data && chown app:app /data
WORKDIR /data
COPY --from=build /out/gensokyo-mcp /app/gensokyo-mcp
ENV ONEBOT_WS_URL=ws://sealdice:18081/ws \
    ONEBOT_BACKEND_ID=sealdice \
    LLM_BRIDGE_DATA_DIR=/data
VOLUME ["/data"]
EXPOSE 8090
HEALTHCHECK --interval=15s --timeout=3s --start-period=10s --retries=3 \
  CMD wget -q -O /dev/null http://127.0.0.1:8090/healthz || exit 1
USER app
ENTRYPOINT ["/app/gensokyo-mcp"]
CMD ["-t", "http", "-addr", ":8090"]
